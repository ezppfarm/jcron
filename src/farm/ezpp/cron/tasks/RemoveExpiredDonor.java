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

public class RemoveExpiredDonor extends CronTask {

	@Override
	public void task() {
		Logger.log("removing expired donator ranks and badges: ", true, true);
		try {
			long time = System.currentTimeMillis() / 1000;
			PreparedStatement ps = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("SELECT id, privileges FROM users WHERE privileges & 4 AND donor_expire < ?");
			ps.setLong(1, time);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				int userid = rs.getInt("id");
				int privileges = rs.getInt("privileges");
				if((privileges & 16) > 0)
					continue;
				
				PreparedStatement ps2 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("UPDATE users SET privileges = privileges - 4 WHERE id = ?");
				ps2.setInt(1, userid);
				ps2.execute();
				PreparedStatement ps3 = (PreparedStatement) MySQLAPI.getInstance().getConnection().prepareStatement("DELETE FROM `user_badges` WHERE badge = 31 AND user = ?");
				ps3.setInt(1, userid);
				ps3.execute();
			}
			Logger.log("done!", false, false);
			markDone();
		} catch (SQLException e) {
			Logger.log("error! " + e.getLocalizedMessage(), false, false);
		}
		
	}

}
