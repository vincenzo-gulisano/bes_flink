package bes_flink;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SinkSocket extends RichSinkFunction<Tuple4<Long, Long, Long, Double>> {

	static Logger LOG = LoggerFactory.getLogger(SinkSocket.class);

	private static final long serialVersionUID = 1L;
	private final String hostName;
	private final int port;
	private final SerializableObject lock = new SerializableObject();
	private transient Socket client;
	private transient PrintWriter outputStream;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private boolean firstSentReported = false;

	public SinkSocket(String hostName, int port) {
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
	public void invoke(Tuple4<Long, Long, Long, Double> value) throws Exception {
		// String msg = serialize(value);
		// System.out.println("Sending " + msg);
		outputStream.println(serialize(value));
		// outputStream.flush();

		if (!firstSentReported) {
			firstSentReported = true;
			LOG.info("First output tuple sent");
		}
	}
}