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
import farm.ezpp.cron.tasks.RemoveScoresFromUnrankedBeatmapsTask.Beatmap;

public class CacheTopPlaysTask extends CronTask {

	public Vector<TopScore> topscores = new Vector<TopScore>();

	@Override
	public void task() {
		Logger.log("caching topscores:", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			String queryString = "DELETE FROM `topscores` WHERE 1";
			s.execute(queryString);
		} catch (SQLException ex) {
			Logger.log(" error! " + ex.getLocalizedMessage(), false, false);
		}
		doSTD();
	}

	public void doSTD() {
		topscores.add(getTopScore(TopScoreMode.STANDART, TopScoreType.VANILLA));
		topscores.add(getTopScore(TopScoreMode.STANDART, TopScoreType.RELAX));
		topscores.add(getTopScore(TopScoreMode.STANDART, TopScoreType.AUTO));
		Logger.log(" .", false, true);
		doTAIKO();
	}

	public void doTAIKO() {
		topscores.add(getTopScore(TopScoreMode.TAIKO, TopScoreType.VANILLA));
		topscores.add(getTopScore(TopScoreMode.TAIKO, TopScoreType.RELAX));
		Logger.log(" .", false, true);
		doCTB();
	}

	public void doCTB() {
		topscores.add(getTopScore(TopScoreMode.CTB, TopScoreType.VANILLA));
		topscores.add(getTopScore(TopScoreMode.CTB, TopScoreType.RELAX));
		Logger.log(" .", false, true);
		doMANIA();
	}

	public void doMANIA() {
		topscores.add(getTopScore(TopScoreMode.MANIA, TopScoreType.VANILLA));
		Logger.log(" .", false, true);
		setBeatmapNames();
	}

	public void setBeatmapNames() {
		for (TopScore ts : topscores) {
			Beatmap bm = getBeatmapFromMD5(ts.beatmap_md5);
			ts.beatmap_name = bm.name;
			ts.beatmap_id = bm.id;
		}
		Logger.log(" .", false, true);
		insertIntoDB();
	}

	public void insertIntoDB() {
		for (TopScore ts : topscores) {
			try {
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("INSERT INTO `topscores`(`player`, `song_name`, `song_id`, `pp`, `type`) VALUES (?, ?, ?, ?, ?)");
				ps.setString(1, ts.username);
				ps.setString(2, ts.beatmap_name);
				ps.setInt(3, ts.beatmap_id);
				ps.setInt(4, ts.pp);
				ps.setString(5, ts.type.name());
				ps.execute();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		Logger.log(" done!", false, false);
		markDone();
	}

	public TopScore getTopScore(int mode, int type) {
		try {
			String table = "scores";
			if (type == TopScoreType.RELAX) {
				table = "scores_relax";
			} else if (type == TopScoreType.AUTO) {
				table = "scores_auto";
			}
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			String queryString = "SELECT users.username, pp, beatmap_md5 FROM " + table + " LEFT JOIN users ON " + table + ".userid = users.id WHERE users.privileges > 2 AND play_mode = " + mode + " ORDER BY pp DESC LIMIT 1";
			ResultSet rs = s.executeQuery(queryString);
			if (rs.first()) {
				String user = rs.getString("username");
				int pp = rs.getInt("pp");
				String beatmap = rs.getString("beatmap_md5");

				TopScoreModeType modeType = TopScoreModeType.STD_VANILLA;

				if (type == TopScoreType.RELAX) {
					if (mode == TopScoreMode.STANDART) {
						modeType = TopScoreModeType.STD_RELAX;
					} else if (mode == TopScoreMode.TAIKO) {
						modeType = TopScoreModeType.TAIKO_RELAX;
					} else if (mode == TopScoreMode.CTB) {
						modeType = TopScoreModeType.CTB_RELAX;
					}
				} else if (type == TopScoreType.AUTO) {
					if (mode == TopScoreMode.STANDART) {
						modeType = TopScoreModeType.STD_AUTO;
					}
				} else {
					if (mode == TopScoreMode.TAIKO) {
						modeType = TopScoreModeType.TAIKO_VANILLA;
					} else if (mode == TopScoreMode.CTB) {
						modeType = TopScoreModeType.CTB_VANILLA;
					} else if (mode == TopScoreMode.MANIA) {
						modeType = TopScoreModeType.MANIA_VANILLA;
					}
				}

				return new TopScore(user, beatmap, pp, modeType);
			}
			return null;
		} catch (SQLException e) {
			Logger.log(" error! " + e.getLocalizedMessage(), false, false);
			return null;
		}
	}

	public Beatmap getBeatmapFromMD5(String md5) {
		return CronExecutor.beatmaps.stream().filter(bm -> bm.beatmap_md5.equals(md5)).findAny().orElse(new Beatmap("null", "null", 0, 0));
	}

	public class TopScore {

		String username;
		String beatmap_md5;
		int beatmap_id;
		String beatmap_name;
		int pp;
		TopScoreModeType type;

		public TopScore(String username, String beatmap_md5, int pp, TopScoreModeType type) {
			this.username = username;
			this.beatmap_md5 = beatmap_md5;
			this.pp = pp;
			this.type = type;
		}

	}

	public enum TopScoreModeType {
		STD_VANILLA, STD_RELAX, STD_AUTO, TAIKO_VANILLA, TAIKO_RELAX, CTB_VANILLA, CTB_RELAX, MANIA_VANILLA, MANIA
	}

	public class TopScoreMode {
		public static final int STANDART = 0;
		public static final int TAIKO = 1;
		public static final int CTB = 2;
		public static final int MANIA = 3;
	}

	public class TopScoreType {
		public static final int VANILLA = 0;
		public static final int RELAX = 1;
		public static final int AUTO = 2;
	}

}
