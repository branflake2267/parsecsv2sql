package com.tribling.csv2sql.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnData {

  // column data types 
  public static final int FIELDTYPE_TEXT = 1;
  public static final int FIELDTYPE_VARCHAR = 2;
  public static final int FIELDTYPE_SMALLINT = 3;
  public static final int FIELDTYPE_INT = 4;
  public static final int FIELDTYPE_BITINT = 5;
  public static final int FIELDTYPE_DECIMAL = 6;
  public static final int FIELDTYPE_DATETIME = 7;
  
  private boolean isPrimaryKey = false;
  
  // column name
	public String column = "";
	
	// column field type - like INTEGER DEFAULT 0
	public String columnType = "TEXT";
	
	// column field length for the given column type
	public int lengthChar = 0;
	
	// columns associated value
	private String value = null;
	
	// true:overwrite any value false:only update on blank
	private boolean overWriteOnUpdate = true;
	
	/**
	 * constructor
	 */
	public ColumnData() {
	}

	public void setValue(String value) {
	  this.value = value;
	}
	
	public String getValue() {
	  return value;
	}
	
	public String getColumnName() {
	  return column;
	}
	
	public void setColumnName(String column) {
	  this.column = column;
	}

	public void setIsPrimaryKey(boolean b) {
	  isPrimaryKey = b;
	}
	
	public boolean getIsPrimaryKey() {
	  return isPrimaryKey;
	}
	
	/**
	 * set column type and extract length
	 * 
	 * @param columnType
	 */
	public void setType(String columnType) {
		this.columnType = columnType.toLowerCase();
		
		String regex = "([0-9]+)";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(columnType);
		boolean found = m.find();

		String len = "";
		if (found == true) {
			len = m.group(1);
		}
		
		if (len.length() > 0) {
			lengthChar = Integer.parseInt(len);
		}	
	}

	public int getCharLength() {
	  return lengthChar;
	}
	
	/**
	 * test value length to see if it will fit
	 * 
	 * @param value
	 * @return
	 */
	public int testValue(String value) {
		
		int resize = 0;
		if (columnType.contains("text")) {
			resize = testText(value);
			
		} else if (columnType.contains("varchar")) {
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
