package com.tribling.csv2sql.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tribling.csv2sql.lib.StringUtil;

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
	private boolean overwriteOnlyWhenBlank = true;
	
	// set the value using regex
	private String regex = null;
	
	// table that the column resides in, optional
	private String table = null;
	
	/**
	 * constructor
	 */
	public ColumnData() {
	}

	public void setValue(String value) {
	  // use regex to get the value
	  if (regex != null) {
	    value = StringUtil.getValue(regex, value);
	  }
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
	
	public void setOverwriteWhenBlank(boolean b) {
	  this.overwriteOnlyWhenBlank = b;
	}
	
	public boolean getOverwriteWhenBlank() {
	  return this.overwriteOnlyWhenBlank;
	}
	
	public void setRegex(String regex) {
	  this.regex = regex;
	}
	
	public String getRegex() {
	  return regex;
	}
	
	public void setTable(String table) {
	  this.table = table;
	}
	
	public String getTable() {
	  return this.table;
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
	
	
  /**
   * find primary key column name
   * 
   * @param columnData
   * @return
   */
  public static String getColumnNameOfPrimaryKey(ColumnData[] columnData) {
    String s = "";
    for (int i=0; i < columnData.length; i++) {
      if (columnData[i].getIsPrimaryKey() == true) {
        s = columnData[i].getColumnName();
        break;
      }
    }
    return s;
  }
  
  /**
   * get the index of primary key 
   * 
   * @param columnData
   * @return
   */
  public static int getIndexOfPrimaryKey(ColumnData[] columnData) {
    int f = -1;
    for (int i=0; i < columnData.length; i++) {
      if (columnData[i].getIsPrimaryKey() == true) {
        f = i;
        break;
      }
    }
    return f;
  }
  
  /**
   * get the value of the primary key
   * 
   * @param columnData
   * @return
   */
  public static String getValueOfPrimaryKey(ColumnData[] columnData) {
    int indexPrimKey = getIndexOfPrimaryKey(columnData);
    String value = columnData[indexPrimKey].getValue();
    return value;
  }
  

	
}
