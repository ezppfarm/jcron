package farm.ezpp.cron;

public class Logger {

	private static String prefix = "jcron > ";

	public static void log(String message, boolean printPrefix) {
		System.out.println((printPrefix ? prefix : "") + message);
	}

	public static void log(String message, boolean printPrefix, boolean inline) {
		if (inline)
			System.out.print((printPrefix ? prefix : "") + message);
		else
			System.out.println((printPrefix ? prefix : "") + message);
	}

	public static void warn(String message) {
		System.out.println("[WARN] " + prefix + message);
	}

	public static void error(String message) {
		System.out.println("[ERROR]" + prefix + message);
	}

	public static String getPrefix() {
		return prefix;
	}

}
