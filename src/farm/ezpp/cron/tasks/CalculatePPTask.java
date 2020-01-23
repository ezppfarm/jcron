package farm.ezpp.cron.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import farm.ezpp.cron.CronExecutor;
import farm.ezpp.cron.CronTask;
import farm.ezpp.cron.Logger;
import farm.ezpp.cron.mysql.MySQLAPI;

public class CalculatePPTask extends CronTask {

	public LinkedHashMap<Integer, Integer> pp_std_vanilla = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_std_relax = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_std_auto = new LinkedHashMap<Integer, Integer>();

	public LinkedHashMap<Integer, Integer> pp_taiko_vanilla = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_taiko_relax = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_taiko_auto = new LinkedHashMap<Integer, Integer>();

	public LinkedHashMap<Integer, Integer> pp_ctb_vanilla = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_ctb_relax = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_ctb_auto = new LinkedHashMap<Integer, Integer>();

	public LinkedHashMap<Integer, Integer> pp_mania_vanilla = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_mania_relax = new LinkedHashMap<Integer, Integer>();
	public LinkedHashMap<Integer, Integer> pp_mania_auto = new LinkedHashMap<Integer, Integer>();

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
		Logger.log("calculating vanilla PP: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT scores.userid, pp, scores.play_mode FROM scores INNER JOIN users ON users.id=scores.userid JOIN beatmaps USING(beatmap_md5) WHERE completed = 3 AND ranked >= 2 AND pp IS NOT NULL ORDER BY pp DESC");
			while (rs.next()) {
				int userID = rs.getInt("userid");
				int pp = rs.getInt("pp");
				int playmode = rs.getInt("play_mode");
				switch (playmode) {
				case 1: // Taiko
					int currentScorePP_taiko = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_taiko_vanilla.size()));
					pp_taiko_vanilla.put(userID, pp_taiko_vanilla.getOrDefault(userID, 0) + currentScorePP_taiko);
					break;
				case 2: // CTB
					int currentScorePP_ctb = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_ctb_vanilla.size()));
					pp_ctb_vanilla.put(userID, pp_ctb_vanilla.getOrDefault(userID, 0) + currentScorePP_ctb);
					break;
				case 3: // Mania
					int currentScorePP_mania = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_mania_vanilla.size()));
					pp_mania_vanilla.put(userID, pp_mania_vanilla.getOrDefault(userID, 0) + currentScorePP_mania);
					break;
				default: // osu!
					int currentScorePP_std = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_std_vanilla.size()));
					pp_std_vanilla.put(userID, pp_std_vanilla.getOrDefault(userID, 0) + currentScorePP_std);
					break;
				}
			}
			
			//STD
			for(Integer userID : pp_std_vanilla.keySet()) {
				int pp = pp_std_vanilla.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_std = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
			}
			//Taiko
			for(Integer userID : pp_taiko_vanilla.keySet()) {
				int pp = pp_taiko_vanilla.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_taiko = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//CTB
			for(Integer userID : pp_ctb_vanilla.keySet()) {
				int pp = pp_ctb_vanilla.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_ctb = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//Mania
			for(Integer userID : pp_mania_vanilla.keySet()) {
				int pp = pp_mania_vanilla.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_mania = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
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
		Logger.log("calculating relax PP: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT scores_relax.userid, pp, scores_relax.play_mode FROM scores_relax INNER JOIN users ON users.id=scores_relax.userid JOIN beatmaps USING(beatmap_md5) WHERE completed = 3 AND ranked >= 2 AND pp IS NOT NULL ORDER BY pp DESC");
			while (rs.next()) {
				int userID = rs.getInt("userid");
				int pp = rs.getInt("pp");
				int playmode = rs.getInt("play_mode");
				switch (playmode) {
				case 1: // Taiko
					int currentScorePP_taiko = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_taiko_relax.size()));
					pp_taiko_relax.put(userID, pp_taiko_relax.getOrDefault(userID, 0) + currentScorePP_taiko);
					break;
				case 2: // CTB
					int currentScorePP_ctb = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_ctb_relax.size()));
					pp_ctb_relax.put(userID, pp_ctb_relax.getOrDefault(userID, 0) + currentScorePP_ctb);
					break;
				case 3: // Mania
					int currentScorePP_mania = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_mania_relax.size()));
					pp_mania_relax.put(userID, pp_mania_relax.getOrDefault(userID, 0) + currentScorePP_mania);
					break;
				default: // osu!
					int currentScorePP_std = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_std_relax.size()));
					pp_std_relax.put(userID, pp_std_relax.getOrDefault(userID, 0) + currentScorePP_std);
					break;
				}
			}
			
			//STD
			for(Integer userID : pp_std_relax.keySet()) {
				int pp = pp_std_relax.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_std_rx = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
			}
			//Taiko
			for(Integer userID : pp_taiko_relax.keySet()) {
				int pp = pp_taiko_relax.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_taiko_rx = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//CTB
			for(Integer userID : pp_ctb_relax.keySet()) {
				int pp = pp_ctb_relax.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_ctb_rx = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//Mania
			for(Integer userID : pp_mania_relax.keySet()) {
				int pp = pp_mania_relax.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_mania_rx = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
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
		Logger.log("calculating auto PP: ", true, true);
		try {
			Statement s = (Statement) MySQLAPI.getInstance().getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT scores_auto.userid, pp, scores_auto.play_mode FROM scores_auto INNER JOIN users ON users.id=scores_auto.userid JOIN beatmaps USING(beatmap_md5) WHERE completed = 3 AND ranked >= 2 AND pp IS NOT NULL ORDER BY pp DESC");
			while (rs.next()) {
				int userID = rs.getInt("userid");
				int pp = rs.getInt("pp");
				int playmode = rs.getInt("play_mode");
				switch (playmode) {
				case 1: // Taiko
					int currentScorePP_taiko = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_taiko_auto.size()));
					pp_taiko_auto.put(userID, pp_taiko_auto.getOrDefault(userID, 0) + currentScorePP_taiko);
					break;
				case 2: // CTB
					int currentScorePP_ctb = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_ctb_auto.size()));
					pp_ctb_auto.put(userID, pp_ctb_auto.getOrDefault(userID, 0) + currentScorePP_ctb);
					break;
				case 3: // Mania
					int currentScorePP_mania = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_mania_auto.size()));
					pp_mania_auto.put(userID, pp_mania_auto.getOrDefault(userID, 0) + currentScorePP_mania);
					break;
				default: // osu!
					int currentScorePP_std = (int) Math.round(Math.round(pp) * Math.pow(0.95, pp_std_auto.size()));
					pp_std_auto.put(userID, pp_std_auto.getOrDefault(userID, 0) + currentScorePP_std);
					break;
				}
			}
			
			//STD
			for(Integer userID : pp_std_auto.keySet()) {
				int pp = pp_std_auto.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_std_auto = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
			}
			//Taiko
			for(Integer userID : pp_taiko_auto.keySet()) {
				int pp = pp_taiko_auto.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_taiko_auto = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//CTB
			for(Integer userID : pp_ctb_auto.keySet()) {
				int pp = pp_ctb_auto.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_ctb_auto = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
			//Mania
			for(Integer userID : pp_mania_auto.keySet()) {
				int pp = pp_mania_auto.getOrDefault(userID, 0);
				PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users_stats SET pp_mania_auto = ? WHERE id = ? LIMIT 1");
				ps.setInt(1, pp);
				ps.setInt(2, userID);
				
			}
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
			markDone();
		}
		Logger.log("done!", false, false);
		markDone();
	}

}
