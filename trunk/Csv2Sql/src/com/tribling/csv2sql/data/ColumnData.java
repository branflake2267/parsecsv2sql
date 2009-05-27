package com.tribling.csv2sql.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnData {

  // column name - make it sql happy
	public String column = "";
	
	// column field type
	public String type = "TEXT";
	
	// column field length for the given type
	public int lengthChar = 0;
	
	/**
	 * constructor - nothing to do
	 */
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
			lengthChar = Integer.parseInt(len);
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
	  // TODO - add the other types of text
		return 0;
	}
	
	/**
	 * test varchar length
	 * 
	 * @param value
	 * @return
	 */
	public int testVarchar(String value) {
		
		int resize = 0;
		if (value.length() > lengthChar) {
			resize = value.length();
		}
		
		return resize;
	}
	
	/**
	 * figure out the length of an int
	 * 
	 * this is goign to be general
	 * 
	 * @param value
	 * @return
	 */
	public int testInt(String value) {
	  
	  // tiny(127) len=0-2
	  
	  // small(32767) len=3-4
	  
	  // medium(8388607) len=4-6
	  
	  // int(2147483647) len=7-9
	  
	  // big int len=9+
	  
    return 0;
	}
	
	
}
