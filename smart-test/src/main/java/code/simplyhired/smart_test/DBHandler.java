package code.simplyhired.smart_test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;


public class DBHandler {
	private static final Logger logger = Logger.getLogger("db");
	
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getConn(String host, int port, String dbName,
			String user, String password) {
		Connection conn = null;

		try {
			conn = DriverManager.getConnection("jdbc:mysql://" + host + ":"
					+ port + "/" + dbName + "?characterEncoding=utf8", user, password);
		} catch (SQLException e) {
			logger.error("Unable to connect to host " + host + " at port " 
					+ port + " db " + dbName + " user " + user + ": " + e.getMessage());
		}

		return conn;
	}
	
	// This lock will automatically be released when the DB connection is terminated
	public static boolean getDBLock(Connection conn, String lockName) {
		boolean haveLock = false;
		PreparedStatement stmt1 = null;
		PreparedStatement stmt2 = null;
		ResultSet resultSet = null;
		
		try {
			// Check whether our DB lock is free
			stmt1 = conn.prepareStatement("select is_free_lock(?);");
			stmt1.setString(1, lockName);
			resultSet = stmt1.executeQuery();
			
			// If the DB lock is free
			if (resultSet.next() && resultSet.getInt(1) == 1) {
				// Acquire it
				stmt2 = conn.prepareStatement("select get_lock(?, 1);");
				stmt2.setString(1, lockName);
				resultSet = stmt2.executeQuery();
				
				// Set to true if we've successfully acquired the lock
				haveLock = (resultSet.next() && resultSet.getInt(1) == 1);
				
				// Clean up
				stmt2.close();
				stmt2 = null;
			}
			
			// Clean up
			stmt1.close();
			stmt1 = null;
		}
		catch (SQLException e) {
			logger.error("Failed to select DB lock: " + e.getMessage());
		}
		finally {
			// Clean up
			if (stmt1 != null) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					logger.error("Couldn't close prepared statement: " + e.getMessage());
				}
				stmt1 = null;
			}
			if (stmt2 != null) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					logger.error("Couldn't close prepared statement: " + e.getMessage());
				}
				stmt2 = null;
			}
		}
		
		return haveLock;
	}

}
