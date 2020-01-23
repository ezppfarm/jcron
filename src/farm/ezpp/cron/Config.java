package farm.ezpp.cron;

import net.md_5.bungee.config.Configuration;
import net.md_5.bungee.config.YamlConfiguration;

import java.io.File;
import java.io.IOException;

public class Config {

	private File configFile;
	private YamlConfiguration config;
	private Configuration cfg;

	public Config(File cfg) {
		configFile = cfg;
		if (!configFile.exists()) {
			try {
				configFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			if (configFile.isDirectory()) {
				try {
					configFile.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		config = new YamlConfiguration();
		try {
			this.cfg = config.load(cfg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void reload() {
		try {
			cfg = config.load(configFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void save() {
		try {
			config.save(cfg, configFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Configuration getConfig() {
		return cfg;
	}
}
