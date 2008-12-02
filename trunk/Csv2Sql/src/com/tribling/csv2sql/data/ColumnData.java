package com.tribling.csv2sql.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnData {

	public String column = "";
	
	public String type = "TEXT";
	
	public int length = 0;
	
	
	public ColumnData() {
	}

	/**
	 * set column type and extract length
	 * 
	 * @param type
	 */
	public void setType(String type) {
		this.type = type.toLowerCase();
		
		String regex = "([0-9]+)";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(type);
		boolean found = m.find();

		String len = "";
		if (found == true) {
			len = m.group(1);
		}
		
		if (len.length() > 0) {
			length = Integer.parseInt(len);
		}	
	}

	/**
	 * test value length to see if it will fit
	 * 
	 * @param value
	 * @return
	 */
	public int testValue(String value) {
		
		int resize = 0;
		if (type.contains("text")) {
			resize = testText(value);
		} else if (type.contains("varchar")) {
			resize = testVarchar(value);
		} 
		
		// TODO test other types - add them here later
		
		return resize;
	}

	/**
	 * test text, nothing to do here
	 * 
	 * @param value
	 * @return
	 */
	public int testText(String value) {
		return 0;
	}
	
	/**
	 * test varchar length
	 * @param value
	 * @return
	 */
	public int testVarchar(String value) {
		
		int resize = 0;
		if (value.length() > length) {
			resize = value.length();
		}
		
		return resize;
	}
	
	
}
