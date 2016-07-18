package bes_flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class Receiver {

	private CountStat rateStat;
	private AvgStat latencyStat;

	public abstract long getTime(String tuple);

	public abstract void receivedTuple(String tuple);

	public void run(String[] args) {

		final ParameterTool params = ParameterTool.fromArgs(args);

		rateStat = new CountStat("", params.getRequired("rateStatFile"), true);
		latencyStat = new AvgStat("", params.getRequired("latencyStatFile"),
				true);

		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(params.getInt("port"));
		} catch (IOException e) {
			System.err.println("Could not listen on port: "
					+ params.getInt("port"));
			System.exit(1);
		}

		Socket clientSocket = null;
		System.out.println("Waiting for connection.....");

		try {
			clientSocket = serverSocket.accept();
		} catch (IOException e) {
			System.err.println("Accept failed.");
			System.exit(1);
		}

		System.out.println("Connection successful");

		BufferedReader in = null;

		boolean gotChar = false;

		try {

			in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			final StringBuilder buffer = new StringBuilder();
			int data;

			System.out.println("Waiting for input.....");

			while ((data = in.read()) != -1) {

				// System.out.print((char) data);
				if (!gotChar) {
					gotChar = true;
					System.out.println("Got at least one char!");
				}

				if (((char) data) == '\r' || ((char) data) == '\n') {

					if (buffer.length() > 0) {
						buffer.setLength(buffer.length() - 1);

						String tuple = buffer.toString();

						rateStat.increase(1);
						latencyStat.add(System.currentTimeMillis()
								- getTime(tuple));
						receivedTuple(tuple);
					}

					buffer.setLength(0);

				} else {
					buffer.append((char) data);
				}

			}

		} catch (IOException e) {
			System.err.print(e);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			try {

				System.out.println("Closing client and server sockets");

				clientSocket.close();
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		rateStat.writeStats();
		latencyStat.writeStats();

	}
}
