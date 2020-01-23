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

public class RemoveScoresFromUnrankedBeatmapsTask extends CronTask {

	public Vector<Score> scores = new Vector<Score>();
	public Vector<Score> scores_relax = new Vector<Score>();
	public Vector<Score> scores_auto = new Vector<Score>();

	public Vector<Score> invalidScores = new Vector<Score>();
	public Vector<Score> invalidScores_relax = new Vector<Score>();
	public Vector<Score> invalidScores_auto = new Vector<Score>();

	@Override
	public void task() {
		Logger.log("getting all beatmaps: ", true, true);
		try {
			/*
			 * 
			 * Hack ist geil :^) Alles wird aus Hack gemacht :3
			 * 
			 */
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT `beatmap_md5`,`ranked`,`song_name`,`beatmap_id` FROM `beatmaps`");
			while (rs.next()) {
				String md5 = rs.getString("beatmap_md5");
				String name = rs.getString("song_name");
				int ranked = rs.getInt("ranked");
				int beatmap_id = rs.getInt("beatmap_id");
				CronExecutor.beatmaps.add(new Beatmap(md5, name, ranked, beatmap_id));
			}
			Logger.log("done!", false, false);
			if (CronExecutor.isVanilla()) {
				doVanilla();
			} else if (CronExecutor.isRelax()) {
				doRelax();
			} else if (CronExecutor.isAuto()) {
				doAuto();
			} else {
				markDone();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
		}

	}

	public void doVanilla() {
		Logger.log("removing unranked vanilla scores: ", true, true);
		try {
			Statement s2 = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs2 = s2.executeQuery("SELECT `beatmap_md5`, `id` FROM `scores` WHERE `completed` > 0");
			while (rs2.next()) {
				String md5 = rs2.getString("beatmap_md5");
				int id = rs2.getInt("id");
				scores.add(new Score(md5, id));
			}

			for (Beatmap beatmap : CronExecutor.beatmaps) {
				for (Score score : scores) {
					if (invalidScores.contains(score))
						continue;
					if (score.beatmap_md5.equals(beatmap.beatmap_md5)) {
						if (beatmap.ranked <= 0 || beatmap.ranked >= 6) {
							invalidScores.add(score);
						}
					}
				}
			}

			for (Score invalid_score : invalidScores) {
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_invalid` SELECT * FROM `scores` WHERE `id`=? LIMIT 1");
				ps.setInt(1, invalid_score.id);
				ps.execute();
				PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores` WHERE `id`=? LIMIT 1");
				ps2.setInt(1, invalid_score.id);
				ps2.execute();
			}
			Logger.log("done!", false, false);
			Logger.log("removed " + invalidScores.size() + " invalid vanilla scores", true);
			if (CronExecutor.isRelax()) {
				doRelax();
			} else if (CronExecutor.isAuto()) {
				doAuto();
			} else {
				markDone();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
		}
	}

	public void doRelax() {
		Logger.log("removing unranked relax scores: ", true, true);
		try {
			Statement s2 = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs2 = s2.executeQuery("SELECT `beatmap_md5`, `id` FROM `scores_relax` WHERE `completed` > 0");
			while (rs2.next()) {
				String md5 = rs2.getString("beatmap_md5");
				int id = rs2.getInt("id");
				scores_relax.add(new Score(md5, id));
			}

			for (Beatmap beatmap : CronExecutor.beatmaps) {
				for (Score score : scores_relax) {
					if (invalidScores_relax.contains(score))
						continue;
					if (score.beatmap_md5.equals(beatmap.beatmap_md5)) {
						if (beatmap.ranked <= 0 || beatmap.ranked >= 6) {
							invalidScores_relax.add(score);
						}
					}
				}
			}
			for (Score invalid_score : invalidScores_relax) {
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_relax_invalid` SELECT * FROM `scores_relax` WHERE `id`=? LIMIT 1");
				ps.setInt(1, invalid_score.id);
				ps.execute();
				PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_relax` WHERE `id`=? LIMIT 1");
				ps2.setInt(1, invalid_score.id);
				ps2.execute();
			}
			Logger.log("done!", false, false);
			Logger.log("removed " + invalidScores_relax.size() + " invalid relax scores", true);
			if (CronExecutor.isAuto()) {
				doAuto();
			} else {
				markDone();
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
		}
	}

	public void doAuto() {
		Logger.log("removing unranked auto scores: ", true, true);
		try {
			Statement s2 = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs2 = s2.executeQuery("SELECT `beatmap_md5`, `id` FROM `scores_auto` WHERE `completed` > 0");
			while (rs2.next()) {
				String md5 = rs2.getString("beatmap_md5");
				int id = rs2.getInt("id");
				scores_auto.add(new Score(md5, id));
			}

			for (Beatmap beatmap : CronExecutor.beatmaps) {
				for (Score score : scores_auto) {
					if (invalidScores_auto.contains(score))
						continue;
					if (score.beatmap_md5.equals(beatmap.beatmap_md5)) {
						if (beatmap.ranked <= 0 || beatmap.ranked >= 6) {
							invalidScores_auto.add(score);
						}
					}
				}
			}

			for (Score invalid_score : invalidScores_auto) {
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `scores_auto_invalid` SELECT * FROM `scores_auto` WHERE `id`=? LIMIT 1");
				ps.setInt(1, invalid_score.id);
				ps.execute();
				PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `scores_auto` WHERE `id`=? LIMIT 1");
				ps2.setInt(1, invalid_score.id);
				ps2.execute();
			}

			Logger.log("done!", false, false);
			Logger.log("removed " + invalidScores_auto.size() + " invalid auto scores", true);
			markDone();
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
		}
	}

	public static class Beatmap {
		String beatmap_md5;
		String name;
		int ranked;
		int id;

		public Beatmap(String md5, String name, int ranked, int id) {
			this.beatmap_md5 = md5;
			this.name = name;
			this.ranked = ranked;
			this.id = id;
		}
	}

	public class Score {
		private String beatmap_md5;
		private int id;

		public Score(String md5, int id) {
			this.beatmap_md5 = md5;
			this.id = id;
		}
	}

}
