package farm.ezpp.cron;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import farm.ezpp.cron.mysql.MySQLAPI;
import farm.ezpp.cron.redis.RedisAPI;
import farm.ezpp.cron.tasks.CacheTopPlaysTask;
import farm.ezpp.cron.tasks.CalculatePPTask;
import farm.ezpp.cron.tasks.CalculateRanksTask;
import farm.ezpp.cron.tasks.CalculateStatsTask;
import farm.ezpp.cron.tasks.FixMultipleScoresTask;
import farm.ezpp.cron.tasks.RemoveExpiredDonor;
import farm.ezpp.cron.tasks.RemoveScoresFromUnrankedBeatmapsTask;
import farm.ezpp.cron.tasks.RemoveScoresFromUnrankedBeatmapsTask.Beatmap;

public class CronExecutor {

	private static Config config;
	private Vector<CronTask> tasks = new Vector<CronTask>();
	long startTime;
	private static boolean vanilla;
	private static boolean relax;
	private static boolean auto;
	public static Vector<Beatmap> beatmaps = new Vector<Beatmap>();


	public CronExecutor() {
		startTime = System.currentTimeMillis();
		File cf = new File("config.yml");
		boolean setDefaults = false;
		if (!cf.exists()) {
			setDefaults = true;
			try {
				cf.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		config = new Config(cf);
		if (setDefaults) {
			config.getConfig().set("mysql.hostname", "localhost");
			config.getConfig().set("mysql.port", 3306);
			config.getConfig().set("mysql.username", "root");
			config.getConfig().set("mysql.password", "changeme");
			config.getConfig().set("mysql.database", "ripple");

			config.getConfig().set("redis.hostname", "localhost");
			config.getConfig().set("redis.port", 6379);
			config.getConfig().set("redis.password", "changeme");
			config.getConfig().set("redis.database", 0);

			config.getConfig().set("option.additional.vanilla", true);
			config.getConfig().set("option.additional.relax", true);
			config.getConfig().set("option.additional.autopilot", true);
//			config.getConfig().set("option.calculateAccuracy", true);
//			config.getConfig().set("option.cacheRankedScore", true);
//			config.getConfig().set("option.cacheTotalHits", true);
//			config.getConfig().set("option.cacheLevel", true);
//			config.getConfig().set("option.deleteOldPasswordResets", true);
//			config.getConfig().set("option.cleanReplays", true);
//			config.getConfig().set("option.populateRedis", true);
			config.getConfig().set("option.removeDonorOnExpired", true);
			config.getConfig().set("option.unrankScoresOnInvalidBeatmaps", true);
			config.getConfig().set("option.fixScoreDuplicates", true);
			config.getConfig().set("option.calculatePP", true);
			config.getConfig().set("option.calculateRanks", true);
			config.getConfig().set("option.calculateStats", true);
			config.getConfig().set("option.cacheTopPlays", true);
//			config.getConfig().set("option.calculateOverallAccuracy", true);
//			config.getConfig().set("option.fixCompletedScores", true);
//			config.getConfig().set("option.fixMultipleCompletedScores", true);
//			config.getConfig().set("option.clearExpiredProfileBackgrounds", true);
//			config.getConfig().set("option.deleteOldPrivateTokens", true);
//			config.getConfig().set("option.setOnlineUsers", true);

			config.save();
			Logger.log("Config created! please configure it!", true);
			return;
		}

		vanilla = config.getConfig().getBoolean("option.additional.vanilla");
		relax = config.getConfig().getBoolean("option.additional.relax");
		auto = config.getConfig().getBoolean("option.additional.autopilot");

		String mysql_host = config.getConfig().getString("mysql.hostname");
		Integer mysql_port = config.getConfig().getInt("mysql.port");
		String mysql_username = config.getConfig().getString("mysql.username");
		String mysql_password = config.getConfig().getString("mysql.password");
		String mysql_database = config.getConfig().getString("mysql.database");

		String redis_host = config.getConfig().getString("redis.hostname");
		Integer redis_port = config.getConfig().getInt("redis.port");
		String redis_password = config.getConfig().getString("redis.password");
		Integer redis_database = config.getConfig().getInt("redis.database");

		// connect to MySQL
		MySQLAPI.getInstance().connect(mysql_host, mysql_port, mysql_username, mysql_password, mysql_database);

		// connect to Redis
		RedisAPI.getInstance().connect(redis_host, redis_port, redis_password, redis_database);
		
		if(config.getConfig().getBoolean("option.removeDonorOnExpired")) {
			addTask(new RemoveExpiredDonor());
		}
		if(config.getConfig().getBoolean("option.unrankScoresOnInvalidBeatmaps")) {
			addTask(new RemoveScoresFromUnrankedBeatmapsTask());
		}
		if (config.getConfig().getBoolean("option.fixScoreDuplicates")) {
			addTask(new FixMultipleScoresTask());
		}
		if (config.getConfig().getBoolean("option.calculatePP")) {
			addTask(new CalculatePPTask());
		}
		if (config.getConfig().getBoolean("option.calculateRanks")) {
			addTask(new CalculateRanksTask());
		}
		if (config.getConfig().getBoolean("option.calculateStats")) {
			addTask(new CalculateStatsTask());
		}
		if(config.getConfig().getBoolean("option.cacheTopPlays")) {
			addTask(new CacheTopPlaysTask());
		}
		nextTask();

	}

	public void nextTask() {
		CronTask nextTask = null;
		for (int i = 0; i < getTasks().size(); i++) {
			CronTask task = getTasks().get(i);
			if (task != null && nextTask == null) {
				if (!task.isDone()) {
					nextTask = task;
					break;
				}
			}
		}

		if (nextTask != null) {
			nextTask.onDone(new DoneTask() {
				@Override
				public void run() {
					nextTask();
				}
			});
			nextTask.task();
		} else {
			exit();
		}
	}

	public void exit() {
		MySQLAPI.getInstance().closeConnection();
		RedisAPI.getInstance().closeConnection();
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		Logger.log("cron took " + duration + "ms", true);
	}

	public static boolean isVanilla() {
		return vanilla;
	}

	public static boolean isRelax() {
		return relax;
	}

	public static boolean isAuto() {
		return auto;
	}

	public static Config getConfig() {
		return config;
	}

	public Vector<CronTask> getTasks() {
		return tasks;
	}

	public void addTask(CronTask task) {
		tasks.add(task);
	}

}
