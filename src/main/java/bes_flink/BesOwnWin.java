package bes_flink;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scalegate.SGTuple;

class BesWindow
		implements
		AggregateWindow<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>> {

	double sum = 0.0;

	@Override
	public AggregateWindow<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>> factory() {
		return new BesWindow();
	}

	@Override
	public void setup() {
	}

	@Override
	public void update(Tuple4<Long, Long, Long, Double> t) {
		sum += t.f3;
	}

	@Override
	public Tuple4<Long, Long, Long, Double> getAggregatedResult(long timestamp,
			String groupby, Tuple4<Long, Long, Long, Double> triggeringTuple) {
		return new Tuple4<Long, Long, Long, Double>(triggeringTuple.f0,
				timestamp, Long.valueOf(groupby), sum);
	}

	@Override
	public long getTimestamp(Tuple4<Long, Long, Long, Double> t) {
		return t.f1;
	}

	@Override
	public String getKey(Tuple4<Long, Long, Long, Double> t) {
		return "" + t.f2;
	}
};

class SGTupleContainer implements SGTuple {

	private Tuple4<Long, Long, Long, Double> t;
	private boolean isFake;

	public SGTupleContainer() {
		this.t = new Tuple4<Long, Long, Long, Double>(0L, 0L, 0L, 0D);
		isFake = true;
	}

	public SGTupleContainer(Tuple5<Long, Long, Long, Double, Integer> t) {
		this.t = new Tuple4<Long, Long, Long, Double>(t.f0, t.f1, t.f2, t.f3);
		isFake = false;
	}

	public boolean isFake() {
		return isFake;
	}

	public Tuple4<Long, Long, Long, Double> getT() {
		return t;
	}

	@Override
	public int compareTo(SGTuple o) {
		if (getTS() == o.getTS()) {
			return 0;
		} else {
			return getTS() > o.getTS() ? 1 : -1;
		}
	}

	@Override
	public long getTS() {
		return t.f1;
	}

}

public class BesOwnWin {

	static Logger LOG = LoggerFactory.getLogger(BesOwnWin.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final int conv_parallelism = params.getInt("conv_parallelism");
		final int agg_parallelism = params.getInt("agg_parallelism");

		SingleOutputStreamOperator<String> in = env
				.addSource(
						new SourceSocket(params.getRequired("injectorIP"),
								params.getInt("injectorPort"), '\n', 0))
				.name("in").startNewChain();

		DataStream<Tuple4<Long, Long, Long, Double>> conv = in
				.flatMap(
						new RichFlatMapFunction<String, Tuple4<Long, Long, Long, Double>>() {

							SimpleDateFormat sdf = new SimpleDateFormat(
									"yyyy-MM-dd HH:mm:ss");

							double bound = 0;

							private CountStat stat;

							public void open(Configuration parameters)
									throws Exception {
								ParameterTool params = (ParameterTool) getRuntimeContext()
										.getExecutionConfig()
										.getGlobalJobParameters();
								bound = params.getDouble("bound");
								LOG.info("Set bound to " + bound);
								sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

								LOG.info("created throughput statistic at  "
										+ params.getRequired("throughputStatFile"));
								stat = new CountStat("", params
										.getRequired("throughputStatFile"),
										true);

							}

							@Override
							public void close() throws Exception {
								stat.writeStats();
							}

							public void flatMap(
									String value,
									Collector<Tuple4<Long, Long, Long, Double>> out)
									throws Exception {

								long ts = -1;
								try {

									long sysTS = Long.valueOf(value.split(",")[0]);
									long meter = Long.valueOf(value.split(",")[1]);
									String tsString = value.split(",")[2];
									ts = sdf.parse(tsString).getTime();
									double cons = Double.valueOf(value
											.split(",")[3]);

									stat.increase(1);
									cons = Math.min(cons, bound);

									// LOG.info("conv " + subtaskIndex
									// + " returning " + sysTS + "," + ts
									// + "," + meter + "," + cons);

									out.collect(new Tuple4<Long, Long, Long, Double>(
											sysTS, ts, meter, cons));
								} catch (Exception e) {
									LOG.warn("Cannot convert input string "
											+ value);
									throw e;
								}

							}
						}).startNewChain().setParallelism(conv_parallelism)
				.name("conv").rebalance();

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> agg = conv
				.keyBy(2)
				.flatMap(
						new RichFlatMapFunction<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>() {

							Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>> aggregate;

							public void open(Configuration parameters)
									throws Exception {
								aggregate = new Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>(
										1000L * 60L * 60L * 24L,
										1000L * 60L * 60L * 24L,
										new BesWindow());
							}

							@Override
							public void flatMap(
									Tuple4<Long, Long, Long, Double> value,
									Collector<Tuple4<Long, Long, Long, Double>> out)
									throws Exception {

								List<Tuple4<Long, Long, Long, Double>> result = aggregate
										.processTuple(value);
								for (Tuple4<Long, Long, Long, Double> t : result)
									out.collect(t);

							}

						}).startNewChain().setParallelism(agg_parallelism)
				.name("agg");

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> map = agg
				.flatMap(
						new RichFlatMapFunction<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>() {

							double bound = 0;
							private Random r;

							public void open(Configuration parameters)
									throws Exception {
								ParameterTool params = (ParameterTool) getRuntimeContext()
										.getExecutionConfig()
										.getGlobalJobParameters();
								bound = params.getDouble("bound");
								LOG.info("Set bound to " + bound);
								r = new Random();
							}

							private double laplace(double lambda) {
								double u = r.nextDouble() * -1 + 0.5;
								return -1
										* (lambda * Math.signum(u) * Math
												.log(1 - 2 * Math.abs(u)));
							}

							@Override
							public void flatMap(
									Tuple4<Long, Long, Long, Double> value,
									Collector<Tuple4<Long, Long, Long, Double>> out)
									throws Exception {

								double noise = laplace(bound);
								out.collect(new Tuple4<Long, Long, Long, Double>(
										value.f0, value.f1, value.f2, value.f3
												+ noise));

							}

						}).startNewChain().name("map");

		map.addSink(
				new SinkSocket(params.getRequired("sinkIP"), params
						.getInt("sinkPort"))).name("sink");

		env.execute("bes");

	}
}
