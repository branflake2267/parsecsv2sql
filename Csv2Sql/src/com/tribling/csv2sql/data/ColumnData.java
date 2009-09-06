package com.tribling.csv2sql.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.WordUtils;

import com.tribling.csv2sql.lib.StringUtil;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class ColumnData {

  // change value case
  public final static int CHANGECASE_LOWER = 1;
  public final static int CHANGECASE_UPPER = 2;
  public final static int CHANGECASE_SENTENCE = 3;
  
  // column data types 
  public static final int FIELDTYPE_DATETIME = 1;
  public static final int FIELDTYPE_VARCHAR = 2;
  public static final int FIELDTYPE_INT_ZEROFILL = 3;
  public static final int FIELDTYPE_INT = 4;
  public static final int FIELDTYPE_DECIMAL = 5;
  public static final int FIELDTYPE_EMPTY = 6;
  public static final int FIELDTYPE_TEXT = 7;
   
  
  // type of index
  public static final int INDEXKIND_DEFAULT = 1;
  public static final int INDEXKIND_FULLTEXT = 2;
  
  // is this column a primary key?
  private boolean isPrimaryKey = false;
  
  // when using multiple columns for identity, for similarity matching
  private boolean usedForIdentity = false;
  
  // column name
  // TODO public access to this var is deprecated, changing to method access
	public String column = "";
	
  private String columnAsSql = null;
  
	// column field type - like INTEGER DEFAULT 0
  // TODO public access to this var is deprecated, changing to method access
	public String columnType = "TEXT";
  // new var to show what type of column it is
  public int fieldType = FIELDTYPE_TEXT;
	
	// column field length for the given column type
  // when type is set, this is discovered
	private int lengthChar = 0;
	
	// columns associated value
	private String value = null;
	
	// set the value as a function
	// this will replace value when looking for retreval
	private String valueIsFunction = null;
	
	// true:overwrite any value false:only update on blank
	private boolean overwriteOnlyWhenBlank = true;
	
	// true: if a zero shows up its ok to overwrite
	private boolean overwriteOnlyWhenZero = true;
	
	// set the value using regex
	private String regex = null;
	
	// table that the column resides in, optional
	private String table = null;
	
	// set with static constant above
	private int changeCase = 0;
	
	// for int type, zerofill??
	private boolean zeroFill = false;
	
	// for decimal
	private int length_left = 0;
	private int length_right = 0;
	
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

	/**
	 * set value of column
	 * 
	 * @param value
	 */
	public void setValue(String value) {
	
	  if (value != null && changeCase > 0) {
	    value = changeCase(value);
	  }
	
	  // use regex to get the value
	  if (regex != null) {
	    value = StringUtil.getValue(regex, value);
	  }
	  this.value = value;
	}
	
	/**
	 * set value of column
	 * 
	 * @param value
	 */
	public void setValue(Long value) {
	  this.value = Long.toString(value);
	}
	
	private String changeCase(String value) {
	  if (changeCase == CHANGECASE_LOWER) {
	    value = value.toLowerCase();
	  } else if (changeCase == CHANGECASE_UPPER) {
	    value = value.toUpperCase();
	  } else if (changeCase == CHANGECASE_SENTENCE) {
	    value = WordUtils.capitalizeFully(value);
	  }
	  return value;
	}
	
	/**
	 * get value
	 *   will return valueIsFunction if set, this will overide value
	 * 
	 * @return
	 */
	public String getValue() {
	  String v = null;
	  if (valueIsFunction != null) {
	    v = valueIsFunction;
	  } else {
	    v = this.value;
	  }
	  return v;
	}
	
	public int getValueLength() {
	  int l = 0;
	  if (value != null) {
	    l = value.length();
	  }
	  return l;
	}
	
	public String getColumnName() {
	  return column;
	}
	
	public void setColumnAsSql() {
	  if (column.matches(".*[\040]as[\040].*") == false) {
	    return;
	  }
	  
	  // save (select *...) as sql
	  columnAsSql = column;
	  
	  String regex = ".*[\040]as[\040](.*)";
	  String c = StringUtil.getValue(regex, column.toLowerCase());
	  if (c != null) {  
	    column = c.trim();
	  }
	  
	}
	
	private String getColumnAsSql() {
	  return columnAsSql;
	}
	
	
	public void setColumnName(String column) {
	  this.column = column;
	  setColumnAsSql(); // in case it has sql AS in it
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
	 * set with constant
	 * 
	 * @param changeCase
	 */
	public void setCase(int changeCase) {
	  this.changeCase = changeCase;
	}
	
	/**
	 * set the value as function
	 * 
	 * @param sqlfunction
	 */
	public void setValueAsFunction(String sqlfunction) {
	  this.valueIsFunction = sqlfunction;
	}
	
	/**
	 * get the value 
	 */
	public String getValueAsFunction() {
	  return valueIsFunction;
	}
	
	/**
	 * is the value set as a function
	 * @return
	 */
	public boolean isFunctionSetForValue() {
	  boolean b = false;
	  if (valueIsFunction != null) {
	    b = true;
	  }
	  return b;
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
		
		if (columnType.contains(",")) {
		  setDecimalLengths(columnType);
		}
	}
	
	public void setType(String columnType, boolean zeroFill) {
	  this.zeroFill = zeroFill;
	  setType(columnType);
	}
	
	/**
	 * get type with no changes
	 * @return
	 */
	public String getType() {
	  return columnType;
	}
	
	/**
	 * recalculate the type 
	 *   like for altering, incase i forced charlen change, and or other change 
	 *   
	 *   I am defining column type in optimize
	 * @return
	 */
	@Deprecated
	public String getTypeNew() {
	  if (columnType.toLowerCase().contains("text") == true) {
	     if (lengthChar <= 255) {
	       columnType = "VARCHAR(" + lengthChar + ") DEFAULT NULL";
	     } else {
	       columnType = "TEXT DEFAULT NULL";
	     }
	  } else if (columnType.toLowerCase().contains("varchar")) {
	    columnType = "VARCHAR(" + lengthChar + ") DEFAULT NULL";
	  } 
	  // TODO more logic needed
	  
	  return columnType;
	}

	/**
	 * get the length of varchar(lengthChar)
	 * 
	 * @return
	 */
	public int getCharLength() {
	  return lengthChar;
	}
	
	/**
	 * change the lengthChar
	 *   used for altering smaller
	 * @param resize
	 */
	public void setCharLength(int resize) {
	  this.lengthChar = resize;
	}
	
	/**
	 * TODO - add all the column types
	 * 
	 * @return
	 */
	public boolean doesValueFitIntoColumn() {
	  columnType = columnType.toLowerCase();

	  boolean b = false;
	  if (columnType.contains("text") == true) {
	    b = doesValueFit_Text();
	  } else if (columnType.contains("varchar") == true) {
	    b = doesValueFit_Varchar();
	  } else if (columnType.contains("int") == true) {
	    b = doesValueFit_Int();
	  } else if (columnType.contains("dec") == true) {
	    b = doesValueFit_Decimal();
	  } else if (columnType.contains("datetime") == true) {
	    b = doesValueFit_DateTime();
	  } else {
	    b = true;
	  }
	  // TODO - add more types
	  
	  return b;
	}
	
	private boolean doesValueFit_Text() {
	  boolean b = false;
	  if (value == null) {
	    b = true;
	  } else if (value.length() <= 65536) { //65536 bytes 2^16
	    b = true;
	  }
	  return b;
	}
	
	private boolean doesValueFit_Varchar() {
	  boolean b = false;
	  if (value == null) {
	    b = true;
	  } else if (value.length() <= lengthChar) { // 255 bytes
	    b = true;
	  }
	  return b;
	}
	
	private boolean doesValueFit_Int() {
	  boolean b = true;
	  // TODO
	  return b;
	}

	private boolean doesValueFit_Decimal() {
	  boolean b = true;
	  // TODO
	  return b;
	}
	
	private boolean doesValueFit_DateTime() {
	  boolean b = true;
	  // TODO
	  return b;
	}
	
	/**
	 * alter the column size if need be
	 */
	public void alterColumnSizeBiggerIfNeedBe(DatabaseData dd) {
	  if (value == null) {
	    return;
	  }
	  // will the data fit
	  boolean b = doesValueFitIntoColumn();
	  if (b == true) {
	    return;
	  }
	  // alter column size
	  alterColumnToBiggerSize(dd);
	}
	
	private void alterColumnToBiggerSize(DatabaseData dd) {
	  int l = value.getBytes().length;
	  
	  if (l >= 255) {
	    setType("TEXT DEFAULT NULL");
	  } else if (columnType.contains("varchar") == true) {
	    setType("VARCHAR(" + l + ") DEFAULT NULL");
	  }
	  
	  MySqlTransformUtil.alterColumn(dd, this);
	}
	
  /**
   * fix the column name < 64 and characters that are SQL friendly
   */
  public void fixName() {
    if (column.length() > 64) {
      column = column.substring(0, 63);
    }
    column = column.trim();
    column = column.replaceAll("#", "_Num");
    column = column.replaceAll("%", "_per");
    column = column.replaceAll("\\.", "_");
    column = column.replaceAll(" ", "_");
    column = column.replaceAll("[^\\w]", "");
    column = column.replaceAll("[\r\n\t]", "");
    column = column.replaceAll("(\\W)", "");
  }
  
  /**
   * TODO - finish this
   * 
   * @param columnType
   * @return
   */
  public int getFieldType(String columnType) {

    String type = columnType.toLowerCase();

    int fieldType = 0;
    if (type.contains("text")) {
      fieldType = ColumnData.FIELDTYPE_VARCHAR;
    } else if (type.contains("date")) {
      fieldType = ColumnData.FIELDTYPE_DATETIME;
    } else if (type.contains("varchar")) {
      fieldType = ColumnData.FIELDTYPE_VARCHAR;
    } else if (type.contains("int")) {
      fieldType = ColumnData.FIELDTYPE_INT;
    } else if (type.contains("double") | type.contains("decimal")) {
      fieldType = ColumnData.FIELDTYPE_DECIMAL;
    } else if (type.length() == 0) {
      fieldType = ColumnData.FIELDTYPE_EMPTY;
    }

    return fieldType;
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
  
  public static String getSql_Names_WSql(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    columnData = prune(columnData, pruneColumnData);
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      String c = "";
      String cn_sql =  columnData[i].getColumnAsSql();
      if (cn_sql != null) {
        c = cn_sql;
      } else {
        c = "`" + columnData[i].getColumnName() + "`";
      }
      sql += c;
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
    columnData = prune(columnData, pruneColumnData);
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      String c = "`" + columnData[i].getColumnName() + "`";    
      String v = null;
      if (columnData[i].isFunctionSetForValue() == true) {
        v = columnData[i].getValueAsFunction();
      } else {
        v = "'" + MySqlQueryUtil.escape(columnData[i].getValue()) + "'";
      }

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
  public static String getSql_Update(ColumnData[] columnData) {
    return getSql_Update(columnData, null );
  }
  
  /**
   * get update sql statement
   * 
   * @param columnData
   * @param primaryKeyId
   * @param pruneColumnData
   * @return
   */
  public static String getSql_Update(ColumnData[] columnData, ColumnData[] pruneColumnData) {
    if (columnData == null) {
      return null;
    }
    ColumnData priKeyCol = getPrimaryKey_ColumnData(columnData);
    String where = " WHERE `" + priKeyCol.getColumnName() + "`='" + priKeyCol.getValue() + "'";
    
    String sql = "UPDATE `" + columnData[0].getTable() + "` SET ";
    pruneColumnData = merge(pruneColumnData, priKeyCol);
    sql += getSql(columnData, pruneColumnData);
    sql += where;
    
    return sql;
  }
  
  /**
   * get sql for calculating the max characters length of a column
   * 
   * @param dd
   * @param columnData
   * @return
   */
  public static String getSql_GetMaxCharLength(DatabaseData dd, ColumnData columnData) {
    if (columnData == null) {
      return null;
    }
    String sql = "SELECT MAX(LENGTH(`" + columnData.getColumnName() + "`)) " +
    		"FROM `" + dd.getDatabase() + "`.`" + columnData.getTable() + "`" ;
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
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    
    // loop through columns
    for (int i=0; i < columnData.length; i++) {
      
      if (doesColumnExist(pruneColumnData, columnData[i]) == true) {
        // don't add it if we find it
      } else {
        cols.add(columnData[i]);
      }
      
    }
    
    ColumnData[] r = new ColumnData[cols.size()];
    cols.toArray(r);
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
    Comparator<ColumnData> sort = new ColumnDataComparator(ColumnDataComparator.NAME);
    Arrays.sort(searchColumnData, sort);
    int index = Arrays.binarySearch(searchColumnData, forColumnData, sort);
    boolean b = false;
    if (index >= 0) {
      b = true;
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
   * get the primary column object
   * 
   * @param columnData
   * @return
   */
  public static ColumnData getPrimaryKey_ColumnData(ColumnData[] columnData) {
    int index = getPrimaryKey_Index(columnData);
    ColumnData r = null;
    if (index > -1) {
      r = columnData[index];
    }
    return r;
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
   * get Sql for identities indexing
   * 
   * @param dd
   * @param columnData
   * @return
   */
  public static String getSql_IdentitiesIndex(DatabaseData dd, ColumnData[] columnData) {
    if (columnData == null) {
      return null;
    }
  
    String autoIndexName = "auto_identities";
    
    boolean exists = MySqlTransformUtil.doesIndexExist(dd, columnData[0].getTable(), autoIndexName);
    if (exists == true) {
      return null;
    }
    
    // get columns used first
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    for(int i=0; i < columnData.length; i++) {
      if (columnData[i].getIdentityUse() == true) {
        cols.add(columnData[i]);
      }
    }
    
    int size = 990;
    if (cols.size() > 1) {
      size = (int) size / cols.size();
    }
    
    String columns = "";
    for(int i=0; i < cols.size(); i++) {
      ColumnData col = cols.get(i);
      String c = col.getColumnName();
      
      String len = "";
      if (col.getType().toLowerCase().contains("text") == true) {
        len = "(" + size + ")";
      }
      columns += "`" + c + "`" + len;
      
      if (i < cols.size() - 1) {
        columns += ",";
      }
      
    }
  
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + columnData[0].getTable() + "` " +
      "ADD INDEX `" + autoIndexName + "`(" + columns + ")"; 

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

  /**
   * add object arrays
   * 
   * @param columnData
   * @param addColumnData
   * @return
   */
  public static ColumnData[] merge(ColumnData[] columnData, ColumnData[] addColumnData) {
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    for(int i=0; i < columnData.length; i++) {
      cols.add(columnData[i]);
    }
    for(int i=0; i < addColumnData.length; i++) {
      cols.add(addColumnData[i]);
    }
    ColumnData[] r = new ColumnData[cols.size()];
    cols.toArray(r);
    return r;
  }
  
  /**
   * add column into object array
   * @param columnData
   * @param addColumnData
   * @return
   */
  public static ColumnData[] merge(ColumnData[] columnData, ColumnData addColumnData) {
    ArrayList<ColumnData> cols = new ArrayList<ColumnData>();
    if (columnData != null) {
      for(int i=0; i < columnData.length; i++) {
        cols.add(columnData[i]);
      }
    }
    cols.add(addColumnData);
    ColumnData[] r = new ColumnData[cols.size()];
    cols.toArray(r);
    return r;
  }
  
  /**
   * get alter columns sql
   * 
   * @param dd
   * @param columnData
   * @return
   */
  public static String getSql_AlterColumns(DatabaseData dd, ColumnData[] columnData) {
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + columnData[0].getTable() + "` ";
    sql += getSql_ModifyColumns(columnData) + ";";
    return sql;
  }
  
  /**
   * get modify column sql
   *   like MODIFY COLUMN `Name` varchar(100) DEFAULT NULL, MODIFY COLUMN `TwoLetter` varchar(2)  DEFAULT NULL
   *   
   * @param columnData
   * @return
   */
  public static String getSql_ModifyColumns(ColumnData[] columnData) {
    String sql = "";
    for (int i=0; i < columnData.length; i++) {
      sql += "MODIFY COLUMN `" + columnData[i].getColumnName() + "` " + columnData[i].getType() + " ";
      if (i < columnData.length - 1 ) {
        sql += ",";
      }
    }
    return sql;
  }
  
  private void setDecimalLengths(String s) {
    int l = 0;
    int r = 0;
    if (s.contains(",")) {
      String[] a = s.split(",");
      l = a[0].trim().length();
      r = a[1].trim().length();
    } else {
      l = s.length();
    }
    length_left = l;
    length_right = r;
  }
  
  
  /**
   * test value length to see if it will fit
   * 
   * @param value
   * @return
   */
  @Deprecated
  public int testSizeOfValue(String value) {
    int resize = 0;
    if (columnType.contains("text")) {
      resize = testSize_Text(value);
      
    } else if (columnType.contains("varchar")) {
      resize = testSize_Varchar(value);
    } 
    return resize;
  }

  /**
   * test text, nothing to do here
   * 
   * @param value
   * @return
   */
  @Deprecated
  public int testSize_Text(String value) {
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
  @Deprecated
  public int testSize_Varchar(String value) {
    int resize = 0;
    if (value.length() > lengthChar) {
      resize = value.length();
    }
    return resize;
  }



  
}
