package bes_flink;

import java.io.IOException;

public class BesInjector extends Injector {

	@Override
	public String prepareLine(String line) {
		return System.currentTimeMillis() + "," + line;
	}

	public static void main(String[] args) throws IOException {
		BesInjector inj = new BesInjector();
		inj.run(args);
	}

}
