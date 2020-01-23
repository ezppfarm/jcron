package farm.ezpp.cron;

public class Main {

	public static CronExecutor instance;

	public static void main(String[] args) {
		instance = new CronExecutor();
	}

	public static CronExecutor getInstance() {
		return instance;
	}

}
