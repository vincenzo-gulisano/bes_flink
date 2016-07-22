package bes_flink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.utils.ParameterTool;

public abstract class InjectorBoundMeters {

	public abstract String prepareLine(String line);

	public abstract String getMeter(String line);

	private CountStat stat;

	private long sleep_period;

	private long batch_size;

	private Set<String> meterSet;
	private int meterBound;

	public void run(String[] args) throws IOException {

		final ParameterTool params = ParameterTool.fromArgs(args);

		stat = new CountStat("", params.getRequired("rateStatFile"), true);

		sleep_period = params.getLong("sleepPeriod");
		batch_size = params.getLong("batchSize");
		meterBound = params.getInt("meterBound");

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

			meterSet = new HashSet<String>();

			long batchCount = 0;
			long before = System.currentTimeMillis();

			while ((line = br.readLine()) != null) {

				if (lineCount % 10000 == 0) {
					System.out.println("Line count: " + lineCount);
				}

				String meter = getMeter(line);
				boolean send = true;
				if (meterSet.contains(meter)
						|| (!meterSet.contains(meter) && meterSet.size() < meterBound))
					meterSet.add(meter);
				else
					send = false;

				if (send) {
					out.println(prepareLine(line));

					stat.increase(1);
					batchCount++;
				}

				lineCount++;

				if (batchCount == batch_size) {
					long sleep = sleep_period
							- (System.currentTimeMillis() - before);
					if (sleep > 0)
						Thread.sleep(sleep);
					before = System.currentTimeMillis();
					batchCount = 0;
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
