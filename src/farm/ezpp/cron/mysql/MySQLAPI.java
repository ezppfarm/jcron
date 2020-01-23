package farm.ezpp.cron.mysql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import farm.ezpp.cron.Logger;

public class MySQLAPI {
	private static final MySQLAPI INSTANCE;
	private String host;
	private int port;
	private String user;
	private String password;
	private String database;
	private Connection connection;

	static {
		INSTANCE = new MySQLAPI();
	}

	private MySQLAPI() {
	}

	public static MySQLAPI getInstance() {
		return MySQLAPI.INSTANCE;
	}

	public void connect(final String host, final int port, final String user, final String password, final String database) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.database = database;
		this.openConnection();
	}

	private void openConnection() {
		try {
			if (this.connection != null) {
				this.connection.close();
			}
			Class.forName("com.mysql.jdbc.Driver");
			this.connection = DriverManager.getConnection("jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database, this.user, this.password);
		} catch (Exception e) {
			Logger.error("[MySQLAPI] Error connecting to " + this.host + ":" + this.port + "/" + this.database + ": " + e.getMessage());
			System.exit(0);
			return;
		}
		Logger.log("[MySQLAPI] Connected to " + this.host + ":" + this.port + "/" + this.database, true);
	}
	
	public void closeConnection() {
		try {
			Logger.log("[MySQLAPI] Connection closed!", true);
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Connection getConnection() {
		return connection;
	}

}
