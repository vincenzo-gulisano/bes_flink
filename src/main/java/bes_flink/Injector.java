package bes_flink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class Injector {

	public abstract String prepareLine(String line);

	private CountStat stat;

	private long sleep_period;

	private long batch_size;

	public void run(String[] args) throws IOException {

		final ParameterTool params = ParameterTool.fromArgs(args);

		stat = new CountStat("", params.getRequired("rateStatFile"), true);

		sleep_period = params.getLong("sleepPeriod");
		batch_size = params.getLong("batchSize");

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
		System.out.println("Waiting for input.....");

		PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		BufferedReader br = null;

		long lineCount = 0;

		try {
			br = new BufferedReader(new FileReader(params.getRequired("input")));
			System.out.println("Reading file");
			String line;

			long batchCount = 0;
			long before = System.currentTimeMillis();

			while ((line = br.readLine()) != null) {

				if (lineCount % 10000 == 0) {
					System.out.println("Line count: " + lineCount);
				}
				out.println(prepareLine(line));
				out.flush();
				stat.increase(1);
				batchCount++;

				lineCount++;

				if (batchCount == batch_size) {

					Thread.sleep(sleep_period
							- (System.currentTimeMillis() - before));
					before = System.currentTimeMillis();
				}

			}
		} catch (IOException e) {
			System.err.print(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				br.close();
		}

		out.close();
		clientSocket.close();
		serverSocket.close();
		stat.writeStats();

	}
}
