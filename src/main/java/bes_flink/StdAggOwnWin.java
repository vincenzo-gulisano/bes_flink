package bes_flink;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StdAggOwnWin {

	static Logger LOG = LoggerFactory.getLogger(StdAggOwnWin.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		SingleOutputStreamOperator<String> in = env.addSource(new SourceSocket(
				params.getRequired("injectorIP"),
				params.getInt("injectorPort"), '\n', 0));

		DataStream<Tuple4<Long, Long, Long, Double>> conv = in
				.flatMap(new RichFlatMapFunction<String, Tuple4<Long, Long, Long, Double>>() {

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");

					private CountStat stat;

					public void open(Configuration parameters) throws Exception {
						ParameterTool params = (ParameterTool) getRuntimeContext()
								.getExecutionConfig().getGlobalJobParameters();
						sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

						LOG.info("created throughput statistic at  "
								+ params.getRequired("throughputStatFile"));
						stat = new CountStat("", params
								.getRequired("throughputStatFile"), true);

					}

					@Override
					public void close() throws Exception {
						stat.writeStats();
					}

					public void flatMap(String value,
							Collector<Tuple4<Long, Long, Long, Double>> out)
							throws Exception {

						long ts = -1;
						try {

							long sysTS = Long.valueOf(value.split(",")[0]);
							long meter = Long.valueOf(value.split(",")[1]);
							String tsString = value.split(",")[2];
							ts = sdf.parse(tsString).getTime();
							double cons = Double.valueOf(value.split(",")[3]);

							stat.increase(1);

							out.collect(new Tuple4<Long, Long, Long, Double>(
									sysTS, ts, meter, cons));
						} catch (Exception e) {
							LOG.warn("Cannot convert input string " + value);
							throw e;
						}

					}
				});

		SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> agg = conv
				.flatMap(new RichFlatMapFunction<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>() {

					Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>> aggregate;

					public void open(Configuration parameters) throws Exception {
						aggregate = new Aggregate<Tuple4<Long, Long, Long, Double>, Tuple4<Long, Long, Long, Double>>(
								1000L * 60L * 60L * 24L,
								1000L * 60L * 60L * 24L, new BesWindow());

					}

					@Override
					public void flatMap(Tuple4<Long, Long, Long, Double> value,
							Collector<Tuple4<Long, Long, Long, Double>> out)
							throws Exception {

						List<Tuple4<Long, Long, Long, Double>> result = aggregate
								.processTuple(value);
						for (Tuple4<Long, Long, Long, Double> t : result)
							out.collect(t);

					}

				});

		agg.addSink(new SinkSocket(params.getRequired("sinkIP"), params
				.getInt("sinkPort")));

		env.execute(params.get("AppName"));

	}
}
