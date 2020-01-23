package farm.ezpp.cron.redis;

import farm.ezpp.cron.Logger;
import redis.clients.jedis.Jedis;

public class RedisAPI {
	private static final RedisAPI INSTANCE;
	private String host;
	private int port;
	private String password;
	private int database;
	private Jedis connection;

	static {
		INSTANCE = new RedisAPI();
	}

	private RedisAPI() {
	}

	public static RedisAPI getInstance() {
		return RedisAPI.INSTANCE;
	}

	public void connect(final String host, final int port, final String password, final int database) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.database = database;
		this.openConnection();
	}

	private void openConnection() {
		Jedis connection = new Jedis(host, port);
		if (!password.isEmpty())
			connection.auth(password);
		connection.select(database);
		if (connection.isConnected()) {
			Logger.log("[RedisAPI] Connected to " + this.host + ":" + this.port + "/" + this.database, true);
			this.connection = connection;
		} else {
			Logger.error("[RedisAPI] Error connecting to " + this.host + ":" + this.port + "/" + this.database);
			System.exit(0);
		}
	}

	public void closeConnection() {
		Logger.log("[RedisAPI] Connection closed!", true);
		connection.disconnect();
		connection = null;
	}

	public Jedis getConnection() {
		return connection;
	}

}
