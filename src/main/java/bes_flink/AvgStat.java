package bes_flink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.TreeMap;

public class AvgStat implements Serializable {

	private static final long serialVersionUID = 1L;

	private long sum;
	private long count;
	private TreeMap<Long, Long> stats;

	String id;

	PrintWriter out;
	boolean immediateWrite;

	long prevSec;

	public AvgStat(String id, String outputFile, boolean immediateWrite) {
		this.id = id;
		this.sum = 0;
		this.count = 0;
		this.stats = new TreeMap<Long, Long>();
		this.immediateWrite = immediateWrite;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		prevSec = System.currentTimeMillis() / 1000;

	}

	public void add(long v) {

		long thisSec = System.currentTimeMillis() / 1000;
		while (prevSec < thisSec) {
			if (immediateWrite) {
				out.println(prevSec * 1000 + ","
						+ (count != 0 ? sum / count : -1));
				out.flush();
			} else {
				this.stats.put(prevSec * 1000, (count != 0 ? sum / count : -1));
			}
			sum = 0;
			count = 0;
			prevSec++;
		}

		sum += v;
		count++;

	}

	public void writeStats() {

		long thisSec = System.currentTimeMillis() / 1000;
		while (prevSec < thisSec) {
			if (immediateWrite) {
				out.println(prevSec * 1000 + ","
						+ (count != 0 ? sum / count : -1));
				out.flush();
			} else {
				this.stats.put(prevSec * 1000, (count != 0 ? sum / count : -1));
			}
			sum = 0;
			count = 0;
			prevSec++;
		}

		if (!immediateWrite) {
			try {
				for (Entry<Long, Long> stat : stats.entrySet()) {

					long time = stat.getKey();
					long counter = stat.getValue();

					out.println(time + "," + counter);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();

	}

}
