package farm.ezpp.cron.tasks;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Vector;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import farm.ezpp.cron.CronExecutor;
import farm.ezpp.cron.CronTask;
import farm.ezpp.cron.Logger;
import farm.ezpp.cron.mysql.MySQLAPI;

public class CalculateStatsTask extends CronTask {

	public Vector<Score> userScores = new Vector<Score>();
	public Vector<Score> userScores_relax = new Vector<Score>();
	public Vector<Score> userScores_auto = new Vector<Score>();

	public LinkedHashMap<Integer, Stats> userStats = new LinkedHashMap<Integer, Stats>();
	public LinkedHashMap<Integer, Stats> userStats_relax = new LinkedHashMap<Integer, Stats>();
	public LinkedHashMap<Integer, Stats> userStats_auto = new LinkedHashMap<Integer, Stats>();

	public Vector<Integer> users = new Vector<Integer>();
	public Vector<Integer> users_relax = new Vector<Integer>();
	public Vector<Integer> users_auto = new Vector<Integer>();

	@Override
	public void task() {
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
		Logger.log("calculating vanilla stats: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT users.id as user_id, scores.play_mode, scores.score, scores.completed FROM scores INNER JOIN users ON users.id=scores.userid");
			while (rs.next()) {
				int userID = rs.getInt("user_id");
				int playmode = rs.getInt("play_mode");
				long score = new Long(rs.getInt("score"));
				int completed = rs.getInt("completed");
				if (playmode <= 3 && playmode >= 0)
					userScores.add(new Score(userID, playmode, score, completed));
			}

			for (Score score : userScores) {
				if (!users.contains(score.userid))
					users.add(score.userid);

				Stats stats = userStats.getOrDefault(score.userid, new Stats());

				switch (score.playmode) {
				case 1: // Taiko
					if (score.completed == 3) {
						stats.score_ranked_taiko += score.score;
					}
					stats.score_total_taiko += score.score;
					break;
				case 2: // CTB
					if (score.completed == 3) {
						stats.score_ranked_ctb += score.score;
					}
					stats.score_total_ctb += score.score;
					break;
				case 3: // Mania
					if (score.completed == 3) {
						stats.score_ranked_mania += score.score;
					}
					stats.score_total_mania += score.score;
					break;
				default: // osu!
					if (score.completed == 3) {
						stats.score_ranked_std += score.score;
					}
					stats.score_total_std += score.score;
					break;
				}

			}

			for (Integer userID : users) {
				Stats stats = userStats.getOrDefault(userID, new Stats());

				int level_std = getLevel(stats.score_total_std);
				int level_taiko = getLevel(stats.score_total_taiko);
				int level_ctb = getLevel(stats.score_total_ctb);
				int level_mania = getLevel(stats.score_total_mania);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE `users_stats` SET `ranked_score_std` = ?, `total_score_std` = ?, `ranked_score_taiko` = ?, `total_score_taiko` = ?, `ranked_score_ctb` = ?, `total_score_ctb` = ?, `ranked_score_mania` = ?, `total_score_mania` = ?, `level_std` = ?, `level_taiko` = ?, `level_ctb` = ?, `level_mania` = ? WHERE `id` = ? LIMIT 1");
				ps.setLong(1, stats.score_ranked_std);
				ps.setLong(2, stats.score_total_std);
				ps.setLong(3, stats.score_ranked_taiko);
				ps.setLong(4, stats.score_total_taiko);
				ps.setLong(5, stats.score_ranked_ctb);
				ps.setLong(6, stats.score_total_ctb);
				ps.setLong(7, stats.score_ranked_mania);
				ps.setLong(8, stats.score_total_mania);
				ps.setInt(9, level_std);
				ps.setInt(10, level_taiko);
				ps.setInt(11, level_ctb);
				ps.setInt(12, level_mania);
				ps.setInt(13, userID);
				ps.execute();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
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
		Logger.log("calculating relax stats: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT users.id as user_id, scores_relax.play_mode, scores_relax.score, scores_relax.completed FROM scores_relax INNER JOIN users ON users.id=scores_relax.userid");
			while (rs.next()) {
				int userID = rs.getInt("user_id");
				int playmode = rs.getInt("play_mode");
				long score = new Long(rs.getInt("score"));
				int completed = rs.getInt("completed");
				if (playmode <= 3 && playmode >= 0)
					userScores_relax.add(new Score(userID, playmode, score, completed));
			}
			
			for (Score score : userScores_relax) {
				if (!users_relax.contains(score.userid))
					users_relax.add(score.userid);

				Stats stats = userStats_relax.getOrDefault(score.userid, new Stats());

				switch (score.playmode) {
				case 1: // Taiko
					if (score.completed == 3) {
						stats.score_ranked_taiko += score.score;
					}
					stats.score_total_taiko += score.score;
					break;
				case 2: // CTB
					if (score.completed == 3) {
						stats.score_ranked_ctb += score.score;
					}
					stats.score_total_ctb += score.score;
					break;
				case 3: // Mania
					if (score.completed == 3) {
						stats.score_ranked_mania += score.score;
					}
					stats.score_total_mania += score.score;
					break;
				default: // osu!
					if (score.completed == 3) {
						stats.score_ranked_std += score.score;
					}
					stats.score_total_std += score.score;
					break;
				}

			}

			for (Integer userID : users_relax) {
				Stats stats = userStats_relax.getOrDefault(userID, new Stats());

				int level_std = getLevel(stats.score_total_std);
				int level_taiko = getLevel(stats.score_total_taiko);
				int level_ctb = getLevel(stats.score_total_ctb);
				int level_mania = getLevel(stats.score_total_mania);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE `users_stats` SET `ranked_score_std_rx` = ?, `total_score_std_rx` = ?, `ranked_score_taiko_rx` = ?, `total_score_taiko_rx` = ?, `ranked_score_ctb_rx` = ?, `total_score_ctb_rx` = ?, `ranked_score_mania_rx` = ?, `total_score_mania_rx` = ?, `level_std_rx` = ?, `level_taiko_rx` = ?, `level_ctb_rx` = ?, `level_mania_rx` = ? WHERE `id` = ? LIMIT 1");
				ps.setLong(1, stats.score_ranked_std);
				ps.setLong(2, stats.score_total_std);
				ps.setLong(3, stats.score_ranked_taiko);
				ps.setLong(4, stats.score_total_taiko);
				ps.setLong(5, stats.score_ranked_ctb);
				ps.setLong(6, stats.score_total_ctb);
				ps.setLong(7, stats.score_ranked_mania);
				ps.setLong(8, stats.score_total_mania);
				ps.setInt(9, level_std);
				ps.setInt(10, level_taiko);
				ps.setInt(11, level_ctb);
				ps.setInt(12, level_mania);
				ps.setInt(13, userID);
				ps.execute();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doAuto() {
		Logger.log("calculating auto stats: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT users.id as user_id, scores_auto.play_mode, scores_auto.score, scores_auto.completed FROM scores_auto INNER JOIN users ON users.id=scores_auto.userid");
			while (rs.next()) {
				int userID = rs.getInt("user_id");
				int playmode = rs.getInt("play_mode");
				long score = new Long(rs.getInt("score"));
				int completed = rs.getInt("completed");
				if (playmode <= 3 && playmode >= 0)
					userScores_auto.add(new Score(userID, playmode, score, completed));
			}
			
			for (Score score : userScores_auto) {
				if (!users_auto.contains(score.userid))
					users_auto.add(score.userid);

				Stats stats = userStats_auto.getOrDefault(score.userid, new Stats());

				switch (score.playmode) {
				case 1: // Taiko
					if (score.completed == 3) {
						stats.score_ranked_taiko += score.score;
					}
					stats.score_total_taiko += score.score;
					break;
				case 2: // CTB
					if (score.completed == 3) {
						stats.score_ranked_ctb += score.score;
					}
					stats.score_total_ctb += score.score;
					break;
				case 3: // Mania
					if (score.completed == 3) {
						stats.score_ranked_mania += score.score;
					}
					stats.score_total_mania += score.score;
					break;
				default: // osu!
					if (score.completed == 3) {
						stats.score_ranked_std += score.score;
					}
					stats.score_total_std += score.score;
					break;
				}

			}

			for (Integer userID : users_auto) {
				Stats stats = userStats_auto.getOrDefault(userID, new Stats());

				int level_std = getLevel(stats.score_total_std);
				int level_taiko = getLevel(stats.score_total_taiko);
				int level_ctb = getLevel(stats.score_total_ctb);
				int level_mania = getLevel(stats.score_total_mania);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE `users_stats` SET `ranked_score_std_ap` = ?, `total_score_std_ap` = ?, `ranked_score_taiko_ap` = ?, `total_score_taiko_ap` = ?, `ranked_score_ctb_ap` = ?, `total_score_ctb_ap` = ?, `ranked_score_mania_ap` = ?, `total_score_mania_ap` = ?, `level_std_ap` = ?, `level_taiko_ap` = ?, `level_ctb_ap` = ?, `level_mania_ap` = ? WHERE `id` = ? LIMIT 1");
				ps.setLong(1, stats.score_ranked_std);
				ps.setLong(2, stats.score_total_std);
				ps.setLong(3, stats.score_ranked_taiko);
				ps.setLong(4, stats.score_total_taiko);
				ps.setLong(5, stats.score_ranked_ctb);
				ps.setLong(6, stats.score_total_ctb);
				ps.setLong(7, stats.score_ranked_mania);
				ps.setLong(8, stats.score_total_mania);
				ps.setInt(9, level_std);
				ps.setInt(10, level_taiko);
				ps.setInt(11, level_ctb);
				ps.setInt(12, level_mania);
				ps.setInt(13, userID);
				ps.execute();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		markDone();
	}

	//
	// https://zxq.co/howl/olc2/src/branch/master/script.js
	// function GetLevel
	//
	public static int getLevel(long score) {
		int level = 1;
		for (;;) {
			long lScore = getRequriedScoreForLevel(level);
			if (score < lScore) {
				return level - 1;
			}
			level++;
		}
	}

	//
	// https://zxq.co/howl/olc2/src/branch/master/script.js
	// function GetRequiredScoreForLevel
	//
	public static long getRequriedScoreForLevel(int level) {
		if (level <= 100) {
			if (level > 1) {
				return (long) Math.floor(5000 / 3 * (4 * Math.pow(level, 3) - 3 * Math.pow(level, 2) - level) + Math.floor(1.25 * Math.pow(1.8, level - 60)));
			}
			return 1;
		}
		return 26931190829L + 100000000000L * (level - 100);
	}

	public class Score {
		private int userid, playmode, completed;
		private long score;

		public Score(int userid, int playmode, long score, int completed) {
			this.playmode = playmode;
			this.score = score;
			this.completed = completed;
		}
	}

	public class Stats {
		private long score_total_std, score_total_taiko, score_total_ctb, score_total_mania;
		private long score_ranked_std, score_ranked_taiko, score_ranked_ctb, score_ranked_mania;
	}

}
