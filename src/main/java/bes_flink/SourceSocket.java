package bes_flink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.util.IOUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * A source function that reads strings from a socket. The source will read
 * bytes from the socket stream and convert them to characters, each byte
 * individually. When the delimiter character is received, the function will
 * output the current string, and begin a new string.
 * <p>
 * The function strips trailing <i>carriage return</i> characters (\r) when the
 * delimiter is the newline character (\n).
 * <p>
 * The function can be set to reconnect to the server socket in case that the
 * stream is closed on the server side.
 */
@PublicEvolving
public class SourceSocket implements SourceFunction<String> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(SourceSocket.class);

	/** Default delay between successive connection attempts */
	private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

	/**
	 * Default connection timeout when connecting to the server socket
	 * (infinite)
	 */
	private static final int CONNECTION_TIMEOUT_TIME = 0;

	private final String hostname;
	private final int port;
	private final char delimiter;
	private final long maxNumRetries;
	private final long delayBetweenRetries;

	private transient Socket currentSocket;

	private volatile boolean isRunning = true;

	public SourceSocket(String hostname, int port, char delimiter,
			long maxNumRetries) {
		this(hostname, port, delimiter, maxNumRetries,
				DEFAULT_CONNECTION_RETRY_SLEEP);
	}

	public SourceSocket(String hostname, int port, char delimiter,
			long maxNumRetries, long delayBetweenRetries) {

		this.hostname = hostname;
		this.port = port;
		this.delimiter = delimiter;
		this.maxNumRetries = maxNumRetries;
		this.delayBetweenRetries = delayBetweenRetries;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {

			try (Socket socket = new Socket()) {
				currentSocket = socket;

				LOG.info("Connecting to server socket " + hostname + ':' + port);
				socket.connect(new InetSocketAddress(hostname, port),
						CONNECTION_TIMEOUT_TIME);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				char[] charArray = new char[2048];
				int actualBuffered;

				while (isRunning
						&& (actualBuffered = reader.read(charArray)) != -1) {
					for (int i = 0; i < actualBuffered; i++) {
						// check if the string is complete
						if (charArray[i] != delimiter) {
							buffer.append((char) charArray[i]);
						} else {
							// truncate trailing carriage return
							if (delimiter == '\n'
									&& buffer.length() > 0
									&& buffer.charAt(buffer.length() - 1) == '\r') {
								buffer.setLength(buffer.length() - 1);
							}
							ctx.collect(buffer.toString());
							buffer.setLength(0);
						}
					}
				}
			}

			// if we dropped out of this loop due to an EOF, sleep and retry
			if (isRunning) {
				attempt++;
				if (maxNumRetries == -1 || attempt < maxNumRetries) {
					LOG.warn("Lost connection to server socket. Retrying in "
							+ delayBetweenRetries + " msecs...");
					Thread.sleep(delayBetweenRetries);
				} else {
					// this should probably be here, but some examples expect
					// simple exists of the stream source
					// throw new
					// EOFException("Reached end of stream and reconnects are not enabled.");
					break;
				}
			}
		}

		// collect trailing data
		if (buffer.length() > 0) {
			ctx.collect(buffer.toString());
		}
	}

	@Override
	public void cancel() {
		isRunning = false;

		// we need to close the socket as well, because the Thread.interrupt()
		// function will
		// not wake the thread in the socketStream.read() method when blocked.
		Socket theSocket = this.currentSocket;
		if (theSocket != null) {
			IOUtils.closeSocket(theSocket);
		}
	}
}
