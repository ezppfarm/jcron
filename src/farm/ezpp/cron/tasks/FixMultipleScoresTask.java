package farm.ezpp.cron.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

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
				List<Score> scoresss = scores.stream().filter(score -> score.beatmap_md5.equals(i.beatmap_md5) && score.userid == i.userid && !scores_duplicate.contains(score)).collect(Collectors.toList());
				if (scoresss.size() < 1)
					continue;
				Score highest = new Score(0, 0, "", 0, 0, 0, 0, 0, 0);

				for (Score scoress : scoresss) {
					if (scoress.pp >= highest.pp) {
						highest = scoress;
					}
				}

				if (scoresss.size() - 1 > 1) {
					for (Score rem : scoresss) {
						if (rem.id == highest.id)
							continue;
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_duplicates` SELECT * FROM `scores` WHERE id = ? LIMIT 1");
						ps.setInt(1, rem.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores` WHERE id = ? LIMIT 1");
						ps2.setInt(1, rem.id);
						ps2.execute();
						scores_duplicate.add(rem);
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + scores_duplicate.size() + " vanilla score duplicates", true, false);
		if (CronExecutor.isRelax()) {
			doRelax();
		} else if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doRelax() {
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
				List<Score> scoresss = scores_relax.stream().filter(score -> score.beatmap_md5.equals(i.beatmap_md5) && score.userid == i.userid && !scores_relax_duplicate.contains(score)).collect(Collectors.toList());
				if (scoresss.size() < 1)
					continue;
				Score highest = new Score(0, 0, "", 0, 0, 0, 0, 0, 0);

				for (Score scoress : scoresss) {
					if (scoress.pp >= highest.pp) {
						highest = scoress;
					}
				}

				if (scoresss.size() - 1 > 1) {
					for (Score rem : scoresss) {
						if (rem.id == highest.id)
							continue;
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_relax_duplicates` SELECT * FROM `scores_relax` WHERE id = ? LIMIT 1");
						ps.setInt(1, rem.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_relax` WHERE id = ? LIMIT 1");
						ps2.setInt(1, rem.id);
						ps2.execute();
						scores_relax_duplicate.add(rem);
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + scores_relax_duplicate.size() + " relax score duplicates", true, false);
		if (CronExecutor.isAuto()) {
			doAuto();
		} else {
			markDone();
		}
	}

	public void doAuto() {
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
				scores_auto.add(new Score(id, userid, beatmap_md5, play_mode, score, mods, max_combo, accuracy, pp));
			}

			for (Score i : scores_auto) {
				List<Score> scoresss = scores_auto.stream().filter(score -> score.beatmap_md5.equals(i.beatmap_md5) && score.userid == i.userid && !scores_auto_duplicate.contains(score)).collect(Collectors.toList());
				if (scoresss.size() < 1)
					continue;
				Score highest = new Score(0, 0, "", 0, 0, 0, 0, 0, 0);

				for (Score scoress : scoresss) {
					if (scoress.pp >= highest.pp) {
						highest = scoress;
					}
				}

				if (scoresss.size() - 1 > 1) {
					for (Score rem : scoresss) {
						if (rem.id == highest.id)
							continue;
						PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_auto_duplicates` SELECT * FROM `scores_auto` WHERE id = ? LIMIT 1");
						ps.setInt(1, rem.id);
						ps.execute();

						PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_auto` WHERE id = ? LIMIT 1");
						ps2.setInt(1, rem.id);
						ps2.execute();
						scores_auto_duplicate.add(rem);
					}
				}
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		Logger.log("fixed " + scores_auto_duplicate.size() + " auto score duplicates", true, false);
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
