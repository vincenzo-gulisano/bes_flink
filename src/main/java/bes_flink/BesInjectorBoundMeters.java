package bes_flink;

import java.io.IOException;

public class BesInjectorBoundMeters extends InjectorBoundMeters {

	@Override
	public String prepareLine(String line) {
		return System.currentTimeMillis() + "," + line;
	}

	public static void main(String[] args) throws IOException {
		BesInjectorBoundMeters inj = new BesInjectorBoundMeters();
		inj.run(args);
	}

	@Override
	public String getMeter(String line) {
		return line.split(",")[0];
	}

}
