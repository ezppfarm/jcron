package farm.ezpp.cron.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import com.mysql.jdbc.Statement;

import farm.ezpp.cron.CronExecutor;
import farm.ezpp.cron.CronTask;
import farm.ezpp.cron.Logger;
import farm.ezpp.cron.mysql.MySQLAPI;
import farm.ezpp.cron.redis.RedisAPI;
import redis.clients.jedis.Jedis;

public class CalculateRanksTask extends CronTask {

	public Vector<String> gamemodes = new Vector<String>();
	private Jedis r = RedisAPI.getInstance().getConnection();

	@Override
	public void task() {
		gamemodes.add("std");
		gamemodes.add("taiko");
		gamemodes.add("ctb");
		gamemodes.add("mania");

		r.del("hanayo:country_list");
		
		
		r.del("ripple:leaderboard_auto");
		if (CronExecutor.isVanilla()) {
			doVanilla();
		} else if (CronExecutor.isRelax()) {
			doRelax();
		} else if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doVanilla() {
		Logger.log("calculating vanilla ranks: ", true, true);
		for (String gamemode : gamemodes) {
			r.del("ripple:leaderboard:" + gamemode);
			try {
				Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
				ResultSet rs = s.executeQuery("SELECT users_stats.id, users_stats.pp_" + gamemode + ", users_stats.country FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users_stats.pp_" + gamemode + " > 0 AND users.privileges & 1 ORDER BY pp_" + gamemode + " DESC");
				while (rs.next()) {
					int userID = rs.getInt("id");
					int pp = rs.getInt("pp_" + gamemode);
					String country = rs.getString("country").toLowerCase();

					r.zadd("ripple:leaderboard:" + gamemode, pp, userID + "");

					if (!country.equals("xx")) {
						r.zincrby("hanayo:country_list", 1, country);
						r.zadd("ripple:leaderboard:" + gamemode + ":" + country, pp, userID + "");
					}

				}
			} catch (SQLException e) {
				Logger.log("error! " + e.getLocalizedMessage(), false, false);
				markDone();
			}
		}
		Logger.log("done!", false, false);
		if (CronExecutor.isRelax()) {
			doRelax();
		} else if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doRelax() {
		Logger.log("calculating relax ranks: ", true, true);
		for (String gamemode : gamemodes) {
			r.del("ripple:leaderboard_relax:" + gamemode);
			try {
				Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
				ResultSet rs = s.executeQuery("SELECT users_stats.id, users_stats.pp_" + gamemode + "_rx, users_stats.country FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users_stats.pp_" + gamemode + "_rx > 0 AND users.privileges & 1 ORDER BY pp_" + gamemode + "_rx DESC");
				while (rs.next()) {
					int userID = rs.getInt("id");
					int pp = rs.getInt("pp_" + gamemode + "_rx");
					String country = rs.getString("country").toLowerCase();

					r.zadd("ripple:leaderboard_relax:" + gamemode, pp, userID + "");

					if (!country.equals("xx")) {
						r.zincrby("hanayo:country_list", 1, country);
						r.zadd("ripple:leaderboard_relax:" + gamemode + ":" + country, pp, userID + "");
					}

				}
			} catch (SQLException e) {
				Logger.log("error! " + e.getLocalizedMessage(), false, false);
				markDone();
			}
		}
		Logger.log("done!", false, false);
		if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doAuto() {
		Logger.log("calculating auto ranks: ", true, true);
		for (String gamemode : gamemodes) {
			r.del("ripple:leaderboard_auto:" + gamemode);
			try {
				Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
				ResultSet rs = s.executeQuery("SELECT users_stats.id, users_stats.pp_" + gamemode + "_auto, users_stats.country FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users_stats.pp_" + gamemode + "_auto > 0 AND users.privileges & 1 ORDER BY pp_" + gamemode + "_auto DESC");
				while (rs.next()) {
					int userID = rs.getInt("id");
					int pp = rs.getInt("pp_" + gamemode + "_auto");
					String country = rs.getString("country").toLowerCase();

					r.zadd("ripple:leaderboard_auto:" + gamemode, pp, userID + "");

					if (!country.equals("xx")) {
						r.zincrby("hanayo:country_list", 1, country);
						r.zadd("ripple:leaderboard_auto:" + gamemode + ":" + country, pp, userID + "");
					}

				}
			} catch (SQLException e) {
				Logger.log("error! " + e.getLocalizedMessage(), false, false);
				markDone();
			}
		}
		Logger.log("done!", false, false);
		markDone();
	}

}
