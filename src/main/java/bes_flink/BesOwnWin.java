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
import scalegate.ScaleGate;
import scalegate.ScaleGateAArrImpl;

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

		final int conv_parallelism = 3;
		final int agg_parallelism = 2;

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		SingleOutputStreamOperator<String> in = env
				.socketTextStream(params.getRequired("injectorIP"),
						params.getInt("injectorPort")).name("in")
				.startNewChain();

		DataStream<Tuple5<Long, Long, Long, Double, Integer>> conv = in
				.flatMap(
						new RichFlatMapFunction<String, Tuple5<Long, Long, Long, Double, Integer>>() {

							SimpleDateFormat sdf = new SimpleDateFormat(
									"yyyy-MM-dd HH:mm:ss");

							boolean boundValues = false;
							double bound = 0;
							int subtaskIndex;

							private CountStat stat;

							public void open(Configuration parameters)
									throws Exception {
								ParameterTool params = (ParameterTool) getRuntimeContext()
										.getExecutionConfig()
										.getGlobalJobParameters();
								if (params.has("bound")) {
									boundValues = true;
									bound = Double.valueOf(params.get("bound"));
									LOG.info("Set bound to " + bound);
								}
								sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

								stat = new CountStat("", params
										.getRequired("throughputStatFile"),
										true);
								LOG.info("created throughput statistic at  "
										+ params.getRequired("throughputStatFile"));
								subtaskIndex = getRuntimeContext()
										.getIndexOfThisSubtask();

							}

							@Override
							public void close() throws Exception {
								stat.writeStats();
							}

							public void flatMap(
									String value,
									Collector<Tuple5<Long, Long, Long, Double, Integer>> out)
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
									cons = boundValues ? Math.min(cons, bound)
											: cons;

									LOG.info("conv " + subtaskIndex
											+ " returning " + sysTS + "," + ts
											+ "," + meter + "," + cons);

									out.collect(new Tuple5<Long, Long, Long, Double, Integer>(
											sysTS, ts, meter, cons,
											subtaskIndex));
								} catch (Exception e) {
									LOG.warn("Cannot convert input string "
											+ value);
								}

							}
						}).startNewChain().setParallelism(conv_parallelism)
				.name("conv").rebalance();

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> agg = conv
				.keyBy(2)
				.flatMap(
						new RichFlatMapFunction<Tuple5<Long, Long, Long, Double, Integer>, Tuple4<Long, Long, Long, Double>>() {

							Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>> aggregate;

							ScaleGate sg;
							int subtaskIndex;

							public void open(Configuration parameters)
									throws Exception {
								aggregate = new Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>(
										1000L * 60L * 60L * 24L,
										1000L * 60L * 60L * 24L,
										new BesWindow());
								sg = new ScaleGateAArrImpl(3, conv_parallelism,
										1);
								for (int i = 0; i < conv_parallelism; i++) {
									sg.addTuple(new SGTupleContainer(), i);
								}
								subtaskIndex = getRuntimeContext()
										.getIndexOfThisSubtask();
							}

							@Override
							public void flatMap(
									Tuple5<Long, Long, Long, Double, Integer> value,
									Collector<Tuple4<Long, Long, Long, Double>> out)
									throws Exception {

								LOG.info("agg " + subtaskIndex + " got tuple "
										+ value);

								sg.addTuple(new SGTupleContainer(value),
										value.f4);

								SGTuple readyT = sg.getNextReadyTuple(0);
								while (readyT != null) {
									SGTupleContainer sgtc = (SGTupleContainer) readyT;
									if (!sgtc.isFake()) {

										LOG.info("agg " + subtaskIndex
												+ " tuple " + sgtc.getT()
												+ " is ready!");

										List<Tuple4<Long, Long, Long, Double>> result = aggregate
												.processTuple(sgtc.getT());
										for (Tuple4<Long, Long, Long, Double> t : result)
											out.collect(t);
									}
									readyT = sg.getNextReadyTuple(0);
								}

							}

						}).startNewChain().setParallelism(agg_parallelism)
				.name("agg");

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> map = agg
				.flatMap(
						new RichFlatMapFunction<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>() {

							boolean boundValues = false;
							double bound = 0;
							double maxCons = 0;
							private Random r;

							public void open(Configuration parameters)
									throws Exception {
								ParameterTool params = (ParameterTool) getRuntimeContext()
										.getExecutionConfig()
										.getGlobalJobParameters();
								if (params.has("bound")) {
									boundValues = true;
									bound = Double.valueOf(params.get("bound"));
									LOG.info("Set bound to " + bound);
								}
								maxCons = params.getDouble("maxCons");
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

								double noise = laplace(boundValues ? bound
										: maxCons);
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
