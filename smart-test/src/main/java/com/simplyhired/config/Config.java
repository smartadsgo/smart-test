package com.simplyhired.config;

import java.io.File;

import org.apache.log4j.Logger;


public class Config {
	
	protected static final Logger logger = Logger.getLogger("marketplaces");
	
	public static void load(String configFile) {
		SHWini config = null;
		
		try {
			config = new SHWini(new File(configFile));
		} catch (Exception e) {
			// If we can't get a valid config, bail out and
			// leave the default values alone
			logger.error("Can't get a valid config, using defaults: " + e.getMessage());
			return;
		}
		
		String host = null;
		int port = 0;
		String name = null;
		String user = null;
		String password = null;
		String table = null;
		
		try {
			host = config.get("LookerDB", "host");
			port = Integer.valueOf(config.get("LookerDB", "port"));
			name = config.get("LookerDB", "name");
			user = config.get("LookerDB", "user");
			password = config.get("LookerDB", "password");
			table = config.get("LookerDB", "table");
		} catch (Exception e) {
			logger.error("error loading configs " + e);
		}
		
		if ((host != null) && (port > 0) && (name != null) && (user != null) && (password != null)) {
			LookerDB.HOST = host;
			LookerDB.PORT = port;
			LookerDB.NAME = name;
			LookerDB.USER = user;
			LookerDB.PASSWORD = password;
		}
		
		try {
			host = config.get("InClickDB", "host");
			port = Integer.valueOf(config.get("InClickDB", "port"));
			name = config.get("InClickDB", "name");
			user = config.get("InClickDB", "user");
			password = config.get("InClickDB", "password");
		} catch (Exception e) {
			logger.error("error loading configs " + e);
		}
		
		if (host != null && port > 0 && name != null && user != null && password != null) {
			InClickDB.HOST = host;
			InClickDB.PORT = port;
			InClickDB.NAME = name;
			InClickDB.USER = user;
			InClickDB.PASSWORD = password;
		}
		
		try {
			host = config.get("InClickTargetDB", "host");
			port = Integer.valueOf(config.get("InClickTargetDB", "port"));
			name = config.get("InClickTargetDB", "name");
			user = config.get("InClickTargetDB", "user");
			password = config.get("InClickTargetDB", "password");
		} catch (Exception e) {
			logger.error("error loading configs " + e);
		}
		
		if (host != null && port > 0 && name != null && user != null && password != null) {
			InClickTargetDB.HOST = host;
			InClickTargetDB.PORT = port;
			InClickTargetDB.NAME = name;
			InClickTargetDB.USER = user;
			InClickTargetDB.PASSWORD = password;
		}
		
		try {
			host = config.get("StatsDB", "host");
			port = Integer.valueOf(config.get("StatsDB", "port"));
			name = config.get("StatsDB", "name");
			user = config.get("StatsDB", "user");
			password = config.get("StatsDB", "password");
		} catch (Exception e) {
			logger.error("error loading configs " + e);
		}
		
		if (host != null && port > 0 && name != null && user != null && password != null) {
			StatsDB.HOST = host;
			StatsDB.PORT = port;
			StatsDB.NAME = name;
			StatsDB.USER = user;
			StatsDB.PASSWORD = password;
		}
		
	}
	
	public static class SHEnvironment {
		public static final String SH_PROJECT_NAME = "SH_PROJECT_NAME";
		public static final String SH_COMMIT_HASH = "SH_COMMIT_HASH";
		public static final String SH_COMMIT_TAG = "SH_COMMIT_TAG";
	}
	
	//TODO Change settings to point to dev-db-100 host
	public static class LookerDB {
		public static String HOST = "db-dev-100.ksjc.sh.colo";
		public static int PORT = 3316;
		public static String NAME = "inclick";
		public static String USER = "shsearch";
		public static String PASSWORD = "applepie";
	}
	
	public static class InClickDB {
		public static String HOST = "db-dev-100.ksjc.sh.colo";
		public static int PORT = 3316;
		public static String NAME = "inclick";
		public static String USER = "shsearch";
		public static String PASSWORD = "applepie";
	}
	
	public static class InClickTargetDB {
		public static String HOST = "db-dev-100.ksjc.sh.colo";
		public static int PORT = 3316;
		public static String NAME = "inclick";
		public static String USER = "shsearch";
		public static String PASSWORD = "applepie";
	}
	
	public static class StatsDB {
		public static String HOST = "db-dev-100.ksjc.sh.colo";
		public static int PORT = 3316;
		public static String NAME = "inclick";
		public static String USER = "shsearch";
		public static String PASSWORD = "applepie";
	}
	
}
