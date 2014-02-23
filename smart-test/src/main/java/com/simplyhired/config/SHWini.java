package com.simplyhired.config;

import java.io.File;
import java.io.IOException;

import org.ini4j.InvalidFileFormatException;
import org.ini4j.Wini;

public class SHWini extends Wini {

	private static final long serialVersionUID = 1L;

	public SHWini(File configFile) throws InvalidFileFormatException, IOException {
		super(configFile);
	}
	
	public int getInt(String section, String option, int defaultValue) {
		int value = defaultValue;
		try {
			value = Integer.valueOf(this.get(section, option));
		} catch (Exception e) {
			// Use default value
		}
		return value;
	}
	
	public long getLong(String section, String option, long defaultValue) {
		long value = defaultValue;
		try {
			value = Long.valueOf(this.get(section, option));
		} catch (Exception e) {
			// Use default value
		}
		return value;
	}
	
	public float getFloat(String section, String option, float defaultValue) {
		float value = defaultValue;
		try {
			value = Float.valueOf(this.get(section, option));
		} catch (Exception e) {
			// Use default value
		}
		return value;
	}
	
	public double getDouble(String section, String option, double defaultValue) {
		double value = defaultValue;
		try {
			value = Double.valueOf(this.get(section, option));
		} catch (Exception e) {
			// Use default value
		}
		return value;
	}
	
	public boolean getBool(String section, String option, boolean defaultValue) {
		boolean value = defaultValue;
		String str = this.get(section, option);
		if (str != null) {
			if (str.equalsIgnoreCase("true")) {
				value = true;
			}
			else if (str.equalsIgnoreCase("false")) {
				value = false;
			}
		}
		return value;
	}
	
	public String getString(String section, String option, String defaultValue) {
		String value = defaultValue;
		try {
			value = this.get(section, option);
		} catch (Exception e) {
			// Use default value
		}
		if (value == null) {
			return defaultValue;
		}
		return value;
	}
}
