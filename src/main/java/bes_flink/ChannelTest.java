package bes_flink;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scalegate.SGTuple;
import scalegate.ScaleGate;
import scalegate.ScaleGateAArrImpl;

class ChannelSinkSocket extends RichSinkFunction<String> {

	static Logger LOG = LoggerFactory.getLogger(SinkSocket.class);

	private static final long serialVersionUID = 1L;
	private final String hostName;
	private final int port;
	private final SerializableObject lock = new SerializableObject();
	private transient Socket client;
	private transient PrintWriter outputStream;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private boolean firstSentReported = false;

	public ChannelSinkSocket(String hostName, int port) {
		this.hostName = hostName;
		this.port = port;
	}

	private void createConnection() throws IOException {
		client = new Socket(hostName, port);
		client.setKeepAlive(true);
		client.setTcpNoDelay(true);

		outputStream = new PrintWriter(client.getOutputStream(), true);
	}

	public String serialize(Tuple4<Long, Long, Long, Double> element) {
		String result = element.f0 + "," + sdf.format(new Date(element.f1))
				+ "," + element.f2 + "," + element.f3;
		return result;
	}

	public void open(Configuration parameters) throws Exception {
		try {
			synchronized (lock) {
				createConnection();
			}
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		} catch (IOException e) {
			throw new IOException("Cannot connect to socket server at "
					+ hostName + ":" + port, e);
		}
	}

	@Override
	public void invoke(String value) throws Exception {
		// String msg = serialize(value);
		// System.out.println("Sending " + msg);
		outputStream.println(value);
		// outputStream.flush();

		if (!firstSentReported) {
			firstSentReported = true;
			LOG.info("First output tuple sent");
		}
	}
};

public class ChannelTest {

	static Logger LOG = LoggerFactory.getLogger(ChannelTest.class);

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
		final int agg_parallelism = 2;

		SingleOutputStreamOperator<String> in = env
				.addSource(
						new SourceSocket(params.getRequired("injectorIP"),
								params.getInt("injectorPort"), '\n', 0))
				.name("in").startNewChain();

		DataStream<String> conv = in
				.flatMap(new RichFlatMapFunction<String, String>() {

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");

					boolean boundValues = false;
					double bound = 0;
					int subtaskIndex;

					private CountStat stat;

					public void open(Configuration parameters) throws Exception {
						ParameterTool params = (ParameterTool) getRuntimeContext()
								.getExecutionConfig().getGlobalJobParameters();
						if (params.has("bound")) {
							boundValues = true;
							bound = Double.valueOf(params.get("bound"));
							LOG.info("Set bound to " + bound);
						}
						sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

						LOG.info("created throughput statistic at  "
								+ params.getRequired("throughputStatFile"));
						subtaskIndex = getRuntimeContext()
								.getIndexOfThisSubtask();
						stat = new CountStat("", params
								.getRequired("throughputStatFile")
								+ subtaskIndex, true);

					}

					@Override
					public void close() throws Exception {
						stat.writeStats();
					}

					public void flatMap(String value, Collector<String> out)
							throws Exception {

						stat.increase(1);
						out.collect(value);

					}
				}).startNewChain().setParallelism(conv_parallelism)
				.name("conv").rebalance();

		conv.addSink(
				new ChannelSinkSocket(params.getRequired("sinkIP"), params
						.getInt("sinkPort"))).name("sink");

		env.execute("bes");

	}
}
