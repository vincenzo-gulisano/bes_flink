package bes_flink;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bes {

	static Logger LOG = LoggerFactory.getLogger(Bes.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		if (!params.has("input")) {
			throw new RuntimeException("Input CSV not specified!");
		}

		// TODO who checks timestamp is always increasing?
		// AscendingTimestampExtractor but check for solar hour / legal hour!!!

		// TODO how does Flink manage non aligned windows?

		DataStreamSource<String> in = env.readTextFile(params.get("input"));
		in.flatMap(
				new RichFlatMapFunction<String, Tuple3<Long, Long, Double>>() {

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");

					boolean boundValues = false;
					double bound = 0;

					public void open(Configuration parameters) throws Exception {
						ParameterTool params = (ParameterTool) getRuntimeContext()
								.getExecutionConfig().getGlobalJobParameters();
						if (params.has("bound")) {
							boundValues = true;
							bound = Double.valueOf(params.get("bound"));
							LOG.info("Set bound to " + bound);
						}
						sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
					}

					public void flatMap(String value,
							Collector<Tuple3<Long, Long, Double>> out)
							throws Exception {

						long ts = -1;
						try {
							String tsString = value.split(",")[1];
							ts = sdf.parse(tsString).getTime();

							double cons = Double.valueOf(value.split(",")[2]);

							cons = boundValues ? Math.min(cons, bound) : cons;
							out.collect(new Tuple3<Long, Long, Double>(ts, Long
									.valueOf(value.split(",")[0]), cons));
						} catch (Exception e) {
							LOG.warn("Cannot convert input string " + value);
						}

					}
				})
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Tuple3<Long, Long, Double>>() {

							@Override
							public long extractAscendingTimestamp(
									Tuple3<Long, Long, Double> arg0) {
								return arg0.f0;
							}
						})
				.keyBy(1)
				.timeWindow(Time.of(1, TimeUnit.DAYS))
				.sum(2)
				.flatMap(
						new RichFlatMapFunction<Tuple3<Long, Long, Double>, Tuple3<Long, Long, Double>>() {

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
									Tuple3<Long, Long, Double> value,
									Collector<Tuple3<Long, Long, Double>> out)
									throws Exception {

								double noise = laplace(boundValues ? bound
										: maxCons);
								out.collect(new Tuple3<Long, Long, Double>(
										value.f0, value.f1, value.f2 + noise));

							}

						})
				.addSink(new RichSinkFunction<Tuple3<Long, Long, Double>>() {

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");

					public void open(Configuration parameters) throws Exception {
						sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
					}

					public void invoke(Tuple3<Long, Long, Double> arg0)
							throws Exception {
						LOG.info("Got tuple [" + sdf.format(new Date(arg0.f0))
								+ "," + arg0.f1 + "," + arg0.f2 + "]");
					}
				});

		env.execute("bes");

	}
}
