package com.tribling.csv2sql.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tribling.csv2sql.lib.StringUtil;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;

public class ColumnData {

  // column data types 
  public static final int FIELDTYPE_TEXT = 1;
  public static final int FIELDTYPE_VARCHAR = 2;
  public static final int FIELDTYPE_SMALLINT = 3;
  public static final int FIELDTYPE_INT = 4;
  public static final int FIELDTYPE_BITINT = 5;
  public static final int FIELDTYPE_DECIMAL = 6;
  public static final int FIELDTYPE_DATETIME = 7;
  
  // type of index
  public static final int INDEXKIND_DEFAULT = 1;
  public static final int INDEXKIND_FULLTEXT = 2;
  
  // is this column a primary key?
  private boolean isPrimaryKey = false;
  
  // when using multiple columns for identity, for similarity matching
  private boolean usedForIdentity = false;
  
  // column name
  // TODO public access to this var is deprecated, changing to method access
  @Deprecated
	public String column = "";
	
	// column field type - like INTEGER DEFAULT 0
  // TODO public access to this var is deprecated, changing to method access
  @Deprecated
	public String columnType = "TEXT";
	
	// column field length for the given column type
  // TODO public access to this var is deprecated, changing to method access
  @Deprecated
	public int lengthChar = 0;
	
	// columns associated value
	private String value = null;
	
	// true:overwrite any value false:only update on blank
	private boolean overwriteOnlyWhenBlank = true;
	
	// true: if a zero shows up its ok to overwrite
	private boolean overwriteOnlyWhenZero = true;
	
	// set the value using regex
	private String regex = null;
	
	// table that the column resides in, optional
	private String table = null;
	
	/**
	 * constructor
	 */
	public ColumnData() {
	}
	
	/**
	 * create a column object 
	 * 
	 * @param columnTable table name
	 * @param columnName column name
	 * @param columnType columnType ie. [TEXT DEFAULT NULL]
	 */
	public ColumnData(String columnTable, String columnName, String columnType) {
	  setTable(columnTable);
	  setColumnName(columnName);
	  setType(columnType);
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
	
	public void setOverwriteWhenZero(boolean b) {
    this.overwriteOnlyWhenZero = b;
  }

  public boolean getOverwriteWhenZero() {
    return this.overwriteOnlyWhenZero;
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
	 * set this column as use as an identity, like three columns get set for similarity matching
	 * 
	 * @param b
	 */
	public void setIdentity(boolean b) {
	  this.usedForIdentity = b;
	}
	
	/**
	 * is this column used for identity
	 * 
	 * @return
	 */
	public boolean getIdentityUse() {
	  return usedForIdentity;
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
	
	public String getType() {
	  return columnType;
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
		// TODO number
		// TODO boolean
		// TODO datetime
		// TODO - decimal is size has to items?
		
		return resize;
	}

	/**
	 * test text, nothing to do here
	 * 
	 * @param value
	 * @return
	 */
	public int testText(String value) {
	  // TODO - add the other types of text (length sizes?)
	  // TODO - what types of text are there?
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
	
	public int testDatetime(String value) {
	  return 0;
	}
	
	/**
	 * figure out the length of an int
	 * 
	 * this is goign to be general
	 * 
	 * @param value
	 * @return
	 */
	public int testNumber(String value) {
	  
	  // zero based 0000123434
	  
	  // decimal 12341234.13044
	  
	  // tiny(127) len=0-2
	  
	  // small(32767) len=3-4
	  
	  // medium(8388607) len=4-6
	  
	  // int(2147483647) len=7-9
	  
	  // big int len=9+
	  
    return 0;
	}

  /**
   * fix the column name < 64 and characters that are SQL friendly
   */
  public void fixName() {
    if (column.length() > 64) {
      column = column.substring(0, 63);
    }
    column = column.trim();
    table = table.replaceAll("#", "_Num");
    table = table.replaceAll("%", "_per");
    table = table.replaceAll("\\.", "_");
    table = table.replaceAll(" ", "_");
    table = table.replaceAll("[^\\w]", "");
    table = table.replaceAll("[\r\n\t]", "");
    table = table.replaceAll("(\\W)", "");
  }
  
  /**
   * get column values from sql resultSet
   * 
   * @param result
   * @param columnData
   * @return
   */
  public static ColumnData[] getResult(ResultSet result, ColumnData[] columnData) {
    return getResult(result, columnData, null);
  }
  
  /**
   * get column values from sql resultSet 
   * 
   * @param result
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static ColumnData[] getResult(ResultSet result, ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null | result == null) {
      return null;
    }
    columnData = prune(columnData, pruneColumnData);
    for (int i=0; i < columnData.length; i++) {
      String value = null;
      try {
        value = result.getString(columnData[i].getColumnName());
      } catch (SQLException e) {
        e.printStackTrace();
      }
      columnData[i].setValue(value);
    }
    return columnData;
  }
  
  /**
   * get sql select statement string from columnData like `cola`,`colb`,`colc`,...
   * 
   * @param columnData
   * @return
   */
  public static String getSql_Names(ColumnData[] columnData) {
    return getSql_Names(columnData, null);
  }
  
  /**
   * get sql select statement string from columnData like `cola`,`colb`,`colc`
   *  
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static String getSql_Names(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    columnData = prune(columnData, pruneColumnData);
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      sql += "`" + columnData[i].getColumnName() + "`";
      if (i < columnData.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  /**
   * get column names in csv format "a","b","c"
   * 
   * @param columnData
   * @return
   */
  public static String getCsv_Names(ColumnData[] columnData) {
    return getCsv_Names(columnData, null);
  }
  
  /**
   * get column names in csv format "a","b","c"
   * 
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static String getCsv_Names(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    columnData = prune(columnData, pruneColumnData);
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      sql += "\"" + columnData[i].getColumnName() + "\"";
      if (i < columnData.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  /**
   * get columnData values as csv, "value1", "value2", "value3",...
   * 
   * @param columnData
   * @return
   */
  public static String getCsv_Values(ColumnData[] columnData) {
    return getCsv_Values(columnData, null);
  }
  
  /**
   * get columnData values as csv, "value1", "value2", "value3",...
   * 
   * @param columnData
   * @return
   */
  public static String getCsv_Values(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    columnData = prune(columnData, pruneColumnData);
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      String v = columnData[i].getValue();
      if (v == null) {
        v = "";
      }
      sql += "\"" + v + "\"";
      if (i < columnData.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  /**
   * get columns as sql, `column1`='value1', `column2`='value2', `column3`='value3',...
   * 
   * @param columnData
   * @return
   */
  public static String getSql(ColumnData[] columnData) {
    return getSql(columnData, null);
  }
  
  /**
   * get columns as sql, `column1`='value1', `column2`='value2', `column3`='value3',...
   * 
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static String getSql(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return "";
    }
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      String c = "`" + columnData[i].getColumnName() + "`";
      String v = "'" + MySqlQueryUtil.escape(columnData[i].getValue()) + "'";
      if (columnData[i].getValue() == null) {
        v = "NULL";
      }
      sql += c + "=" + v;
      if (i < columnData.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  /**
   * get Columns as Sql Insert Statement
   * 
   * @param columnData
   * @return
   */
  public static String getSql_Insert(ColumnData[] columnData) {
    return getSql_Insert(columnData, null);
  }
  
  /**
   * get Columns as Sql Insert Statement
   * 
   *   REMEMBER - in most cases primary key will need to be pruned, I decided to keep it out in case i wanted it
   *   columnData = prunePrimaryKey(columnData); This should be done by earlier method
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static String getSql_Insert(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return "";
    }
    columnData = prune(columnData, pruneColumnData);
    String table = columnData[0].getTable();
    String fields = getSql(columnData);
    String sql = "INSERT INTO `" + table + "` SET " + fields + ";";
    return sql;
  }
  
  /**
   * get update sql statement
   * 
   * @param columnData
   * @param primaryKeyId
   * @return
   */
  public static String getSql_Update(ColumnData[] columnData, long primaryKeyId) {
    return getSql_Update(columnData, primaryKeyId, null );
  }
  
  /**
   * get update sql statement
   * 
   * @param columnData
   * @param primaryKeyId
   * @param pruneColumnData
   * @return
   */
  public static String getSql_Update(ColumnData[] columnData, long primaryKeyId, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    String primaryKeyName = getPrimaryKey_Name(columnData);
    String where = " WHERE `" + primaryKeyName + "`='" + primaryKeyId + "'";
    
    String sql = "UPDATE `" + columnData[0].getTable() + "` SET ";
    sql += getSql(columnData, pruneColumnData);
    sql += where;
    
    return sql;
  }
  
  /**
   * prune columns
   * 
   * @param columnData
   * @param pruneColumnData
   * @return
   */
  public static ColumnData[] prune(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (pruneColumnData == null) {
      return columnData;
    }
    ArrayList<ColumnData> newCols = new ArrayList<ColumnData>();
    for (int i=0; i < columnData.length; i++) {
      if (doesColumnExist(pruneColumnData, columnData[i]) == false) {
        newCols.add(columnData[i]);
      }
    }
    ColumnData[] r = (ColumnData[]) newCols.toArray();
    return r;
  }
  
  /**
   * prune PrimaryKey from columnData
   * 
   * @param columnData
   * @return
   */
  public static ColumnData[] prunePrimaryKey(ColumnData[] columnData) {
    if (columnData == null) {
      return null;
    }
    ArrayList<ColumnData> newCols = new ArrayList<ColumnData>();
    for (int i=0; i < columnData.length; i++) {
      if (columnData[i].getIsPrimaryKey() == false) {
        newCols.add(columnData[i]);
      }
    }
    ColumnData[] r = new ColumnData[newCols.size()];
    r = (ColumnData[]) newCols.toArray(r);
    return r;
  }
  
  /**
   * does column exist?
   * 
   * @param searchColumnData - look in these columns
   * @param forColumnData - comparing this name to pruneColumnData
   * 
   * @return
   */
  public static boolean doesColumnExist(ColumnData[] searchColumnData, ColumnData forColumnData) {
    Comparator<ColumnData> sort = new ColumnDataComparator();
    Arrays.sort(searchColumnData, sort);
    int index = Arrays.binarySearch(searchColumnData, forColumnData, sort);
    boolean b = true;
    if (index >= 0) {
      b = false;
    }
    return b;
  }
 
  /**
   * does column name exist?
   * 
   * @param searchColumnData
   * @param forColumnName
   * @return
   */
  public static boolean doesColumnNameExist(ColumnData[] searchColumnData, String forColumnName) {
    if (searchColumnData == null | forColumnName == null) {
      return false;
    }
    boolean b = false;
    for (int i=0; i < searchColumnData.length; i++) {
      if (searchColumnData[i].getColumnName().equals(forColumnName)) {
        b = true;
        break;
      }
    }
    return b;
  }

  /**
   * get the value of the primary key
   * 
   * @param columnData
   * @return
   */
  public static String getPrimaryKey_Value(ColumnData[] columnData) {
    int indexPrimKey = getPrimaryKey_Index(columnData);
    String value = columnData[indexPrimKey].getValue();
    return value;
  }

  /**
   * find primary key column name
   * 
   * @param columnData
   * @return
   */
  public static String getPrimaryKey_Name(ColumnData[] columnData) {
    if (columnData == null) {
      return null;
    }
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
  public static int getPrimaryKey_Index(ColumnData[] columnData) {
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
   * add values into the column Data
   * 
   * @param columnData
   * @param values
   * @return
   */
  public static ColumnData[] addValues(ColumnData[] columnData, String[] values) {
    if (columnData == null) {
      return null;
    }
    for (int i=0; i < columnData.length; i++) {
      columnData[i].setValue(values[i]);
    }
    return columnData;
  }
  
  /**
   * get Identitys Where statement 
   * 
   * @param columnData
   * @return
   */
  public static String getSql_IdentitiesWhere(ColumnData[] columnData) {
    
    // get columns used first
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    for(int i=0; i < columnData.length; i++) {
      if (columnData[i].getIdentityUse() == true) {
        cols.add(columnData[i]);
      }
    }
    
    // create sql where vars
    String sql = "";
    for(int i=0; i < cols.size(); i++) {
      ColumnData col = cols.get(i);
      String c = col.getColumnName();
      String v = col.getValue();
      sql += "`" + c + "`='" + v + "'";
      if (i < cols.size() - 1) {
        sql += " AND ";
      }
    }

    return sql;
  }
  
  /**
   * get just the columns that are identities
   * @param columnData
   * @return
   */
  public static ColumnData[] getColumns_Identities(ColumnData[] columnData) {
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    for(int i=0; i < columnData.length; i++) {
      if (columnData[i].getIdentityUse() == true) {
        cols.add(columnData[i]);
      }
    }
    ColumnData[] r = new ColumnData[cols.size()];
    cols.toArray(r);
    return r;
  }
  
}
