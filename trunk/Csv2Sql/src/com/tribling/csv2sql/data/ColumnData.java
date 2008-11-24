package com.tribling.csv2sql.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnData {

	public String column;
	
	public String type;
	
	public int length;
	
	
	public ColumnData() {
	}

	/**
	 * set column type and extract length
	 * 
	 * @param type
	 */
	public void setType(String type) {
		this.type = type;
		
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
	 * compare columns to see if they values will all fit
	 * @param columns
	 * @param col
	 * @param val
	 * @return
	 */
	public boolean compare(ColumnData[] columns, String[] col, String[] val) {
		
		return false;
	}
	
	
}
