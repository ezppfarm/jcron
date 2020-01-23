package farm.ezpp.cron.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import farm.ezpp.cron.CronExecutor;
import farm.ezpp.cron.CronTask;
import farm.ezpp.cron.Logger;
import farm.ezpp.cron.mysql.MySQLAPI;

public class FixMultipleScoresTask extends CronTask {

	private Vector<Score> scores = new Vector<Score>();
	private Vector<Score> scores_duplicate = new Vector<Score>();

	private Vector<Score> scores_relax = new Vector<Score>();
	private Vector<Score> scores_relax_duplicate = new Vector<Score>();

	private Vector<Score> scores_auto = new Vector<Score>();
	private Vector<Score> scores_auto_duplicate = new Vector<Score>();

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
		int count = 0;
		Logger.log("fixing multiple vanilla scores: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT `id`, `beatmap_md5`, `userid`, `score`, `max_combo`, `mods`, `play_mode`, `accuracy`, `pp` FROM `scores` WHERE completed = 3");
			while (rs.next()) {
				int id = rs.getInt("id");
				String beatmap_md5 = rs.getString("beatmap_md5");
				int userid = rs.getInt("userid");
				long score = new Long(rs.getInt("score"));
				int max_combo = rs.getInt("max_combo");
				int mods = rs.getInt("mods");
				int play_mode = rs.getInt("play_mode");
				float accuracy = rs.getInt("accuracy");
				int pp = rs.getInt("pp");
				scores.add(new Score(id, userid, beatmap_md5, play_mode, score, mods, max_combo, accuracy, pp));
			}

			for (Score i : scores) {
				if (scores_duplicate.contains(i))
					continue;
				for (Score j : scores) {
					if (j.id != i.id && j.beatmap_md5.equals(i.beatmap_md5) && j.userid == i.userid && j.play_mode == i.play_mode && j.score == i.score && j.mods == i.mods && j.max_combo == i.max_combo && j.accuracy == i.accuracy) {
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_duplicates` SELECT * FROM `scores` WHERE id = ? LIMIT 1");
						ps.setInt(1, j.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores` WHERE id = ? LIMIT 1");
						ps2.setInt(1, j.id);
						ps2.execute();
						scores_duplicate.add(j);
						count++;
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + count + " vanilla score duplicates", true, false);
		if (CronExecutor.isRelax()) {
			doRelax();
		} else if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doRelax() {
		int count = 0;
		Logger.log("fixing multiple relax scores: ", true, true);
		try {
			
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT `id`, `beatmap_md5`, `userid`, `score`, `max_combo`, `mods`, `play_mode`, `accuracy`, `pp` FROM `scores_relax` WHERE completed = 3");
			while (rs.next()) {
				int id = rs.getInt("id");
				String beatmap_md5 = rs.getString("beatmap_md5");
				int userid = rs.getInt("userid");
				long score = new Long(rs.getInt("score"));
				int max_combo = rs.getInt("max_combo");
				int mods = rs.getInt("mods");
				int play_mode = rs.getInt("play_mode");
				float accuracy = rs.getInt("accuracy");
				int pp = rs.getInt("pp");
				scores_relax.add(new Score(id, userid, beatmap_md5, play_mode, score, mods, max_combo, accuracy, pp));
			}

			for (Score i : scores_relax) {
				if (scores_relax_duplicate.contains(i))
					continue;
				for (Score j : scores_relax) {
					if (j.id != i.id && j.beatmap_md5.equals(i.beatmap_md5) && j.userid == i.userid && j.play_mode == i.play_mode && j.score == i.score && j.mods == i.mods && j.max_combo == i.max_combo && j.accuracy == i.accuracy) {
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_relax_duplicates` SELECT * FROM `scores_relax` WHERE id = ? LIMIT 1");
						ps.setInt(1, j.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_relax` WHERE id = ? LIMIT 1");
						ps2.setInt(1, j.id);
						ps2.execute();
						scores_relax_duplicate.add(j);
						count++;
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + count + " relax score duplicates", true, false);
		if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doAuto() {
		int count = 0;
		Logger.log("fixing multiple auto scores: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT `id`, `beatmap_md5`, `userid`, `score`, `max_combo`, `mods`, `play_mode`, `accuracy`, `pp` FROM `scores_auto` WHERE completed = 3");
			while (rs.next()) {
				int id = rs.getInt("id");
				String beatmap_md5 = rs.getString("beatmap_md5");
				int userid = rs.getInt("userid");
				long score = new Long(rs.getInt("score"));
				int max_combo = rs.getInt("max_combo");
				int mods = rs.getInt("mods");
				int play_mode = rs.getInt("play_mode");
				float accuracy = rs.getInt("accuracy");
				int pp = rs.getInt("pp");
				scores_relax.add(new Score(id, userid, beatmap_md5, play_mode, score, mods, max_combo, accuracy, pp));
			}

			for (Score i : scores_auto) {
				if (scores_auto_duplicate.contains(i))
					continue;
				for (Score j : scores_auto) {
					if (j.id != i.id && j.beatmap_md5.equals(i.beatmap_md5) && j.userid == i.userid && j.play_mode == i.play_mode && j.score == i.score && j.mods == i.mods && j.max_combo == i.max_combo && j.accuracy == i.accuracy && j.pp == i.pp) {
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_auto_duplicates` SELECT * FROM `scores_auto` WHERE id = ? LIMIT 1");
						ps.setInt(1, j.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_auto` WHERE id = ? LIMIT 1");
						ps2.setInt(1, j.id);
						ps2.execute();
						scores_auto_duplicate.add(j);
						count++;
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + count + " auto score duplicates", true, false);
		markDone();
	}

	public class Score {

		private int id;
		private int userid;
		private String beatmap_md5;
		private int play_mode;
		private long score;
		private int mods;
		private int max_combo;
		private float accuracy;
		private int pp;

		public Score(int id, int userid, String beatmap_md5, int play_mode, long score, int mods, int max_combo, float accuracy, int pp) {
			this.id = id;
			this.userid = userid;
			this.beatmap_md5 = beatmap_md5;
			this.play_mode = play_mode;
			this.score = score;
			this.mods = mods;
			this.max_combo = max_combo;
			this.accuracy = accuracy;
			this.pp = pp;
		}
	}

}
