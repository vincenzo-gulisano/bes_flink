package bes_flink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.TreeMap;

public class CountStat implements Serializable {

	private static final long serialVersionUID = 2415520958777993198L;

	private long count;
	private TreeMap<Long, Long> countStats;

	String id;

	PrintWriter out;
	boolean immediateWrite;

	long prevSec;

	public CountStat(String id, String outputFile, boolean immediateWrite) {
		this.id = id;
		this.count = 0;
		this.countStats = new TreeMap<Long, Long>();
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

	public void increase(long v) {

		long thisSec = System.currentTimeMillis() / 1000;
		while (prevSec < thisSec) {
			if (immediateWrite) {
				out.println(prevSec * 1000 + "," + count);
				out.flush();
			} else {
				this.countStats.put(prevSec * 1000, count);
			}
			count = 0;
			prevSec++;
		}
		count += v;
	}

	public void writeStats() {

		long thisSec = System.currentTimeMillis() / 1000;
		while (prevSec < thisSec) {
			if (immediateWrite) {
				out.println(prevSec * 1000 + "," + count);
				out.flush();
			} else {
				this.countStats.put(prevSec * 1000, count);
			}
			count = 0;
			prevSec++;
		}

		if (!immediateWrite) {
			try {
				for (Entry<Long, Long> stat : countStats.entrySet()) {

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
