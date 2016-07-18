package bes_flink;

import java.io.IOException;

public class BesReceiver extends Receiver {

	public BesReceiver() {

	}

	@Override
	public long getTime(String tuple) {
		return Long.valueOf(tuple.split(",")[0]);
	}

	@Override
	public void receivedTuple(String tuple) {
		// System.out.println(tuple);
	}

	public static void main(String[] args) throws IOException {
		BesReceiver rec = new BesReceiver();
		rec.run(args);
	}

}
