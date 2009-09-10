package com.tribling.csv2sql.v2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.lib.StringUtil;
import com.tribling.csv2sql.lib.datetime.DateTimeParser;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

/**
 * 
 * @author BDonnelson
 * 
 */
public class Optimise_v2 extends SQLProcessing_v2 {

  private DestinationData_v2 destinationData = null;
  
  // used to examine the value if its a date, and it can transform it
  private DateTimeParser dtp = new DateTimeParser();
  
  private ColumnData[] columnData = null;

  // resize these columns
  private ArrayList<ColumnData> alterColumns = new ArrayList<ColumnData>();
  
  // when examining the column values, watch the values types
  public int fieldType = ColumnData.FIELDTYPE_TEXT;
  
  // decimal lengths
  public int deca = 0;
  public int decb = 0;
  
  public Optimise_v2(DestinationData_v2 destinationData) {
    this.destinationData = destinationData;
  }
  
  /**
   * optimize all the columns
   */
  public void run() {
    String where = null; // all columns
    columnData = MySqlTransformUtil.queryColumns(destinationData.databaseData, destinationData.table, where);
    if (columnData == null) {
      System.out.println("no columns to optimise");
      System.exit(1);
    }
    process();
    
    alterColumns();
  }
  
  /**
   * optimise only these columns
   * 
   * @param columnData
   */
  public void run(ColumnData[] columnData) {
    if (columnData == null) {
      return;
    }
    this.columnData = columnData; 
    process();
    
    alterColumns();
  }
  
  /**
   * analyze columns to go smaller
   */
  private void process() {
    for (int i=0; i < columnData.length; i++) {
      checkColumn(columnData[i]);
    }
  }
  
  /**
   * check column
   * 
   * @param columnData
   */
  private void checkColumn(ColumnData columnData) {
    
    // skip auto_ and primary key
    if (columnData.getColumnName().matches("Auto_.*") == true) {
      return;
    }
    
    if (columnData.getIsPrimaryKey() == true) {
      return;
    }
    
    // only analyze text columns
    if (destinationData.optimise_TextOnlyColumnTypes == true && columnData.getType().toLowerCase().contains("text") == false) {
      return;
    }
      
    // examine for the best field type (column type) int, decimal, text varchar.
    analyzeColumnType(columnData);
    
    // get character max length in column
    String sql = ColumnData.getSql_GetMaxCharLength(destinationData.databaseData, columnData);
    int maxCharLength = MySqlQueryUtil.queryInteger(destinationData.databaseData, sql);
    
    String newColumnType = getColumnType(columnData, maxCharLength);
    
    // did type change, then alter
    boolean changed = didItChange(columnData, newColumnType);
    
    // TODO - if changing to a datetime, need to transform all values in the column to datetime
    
    if (changed == true) {
      alter(columnData, newColumnType);
    }
     
  }
  
  private void alter(ColumnData columnData, String columnType) {
    
    // when altering dates, make sure every value is transformed to
    if (columnType.toLowerCase().contains("datetime") == true) {
      formatColumn_ToDateTime(columnData);
    }
    
    columnData.setType(columnType);
    
    // store it
    alterColumns.add(columnData);
  }

  private boolean didItChange(ColumnData columnData, String newColumnType) {
    boolean b = true;
    
    String orgColumnType = columnData.getType();
    String orgType = StringUtil.getValue("(.*?\\))", orgColumnType);
    String newType = StringUtil.getValue("(.*?\\))", newColumnType);
    
    if (orgType == null) {
      orgType = orgColumnType;
    }
    
    if (newType == null) {
      newType = newColumnType;
    }
    
    if (orgType.toLowerCase().equals(newType.toLowerCase()) == true) {
      b = false;
    }
    
    return b;
  }
  
  /**
   * sample the columns values and see what type is the best
   * 
   * @param columnData
   */
  private void analyzeColumnType(ColumnData columnData) {
    
    fieldType = 0;  
    deca = 0;
    decb = 0;
    
    String random = "";
    if (destinationData.optimise_RecordsToExamine > 0 && destinationData.optimise_skipRandomExamine == false) {
        random = "ORDER BY RAND()";
    }
    
    // sample values that aren't null
    String ignoreNullValues = "";
    if (destinationData.optimise_ignoreNullFieldsWhenExamining == true) {
      ignoreNullValues = "WHERE (" + columnData.getColumnName() + " IS NOT NULL)";
    }

    // column query
    String sql = "SELECT `" + columnData.getColumnName() + "` " + 
        "FROM " + destinationData.databaseData.getDatabase() + "." + columnData.getTable() + " " +
        "" + ignoreNullValues + " " + random + " " + getLimitQuery() + ";"; 
      
    System.out.println("Analyzing Column For Type: " + columnData.getColumnName() + " query: " + sql);

    try { 
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE); // read row by row 
      ResultSet result = select.executeQuery(sql);
      int i = 0;
      while (result.next()) {
        examineField(result.getString(1));
        i++;
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("SQL Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }
  
  /**
   * what is the value type?
   *   I use chain logic here where, the lowest is text/varchar
   * @param s
   */
  private void examineField(String s) {

    // skip when empty
    if (s == null) {
      // don't even analyze field type when nothing
      return;
    }

    boolean isInt = false;
    boolean isZero = false;
    boolean isDecimal = false;
    boolean isDate = false;
    boolean isText = false;
    boolean isEmpty = false;
    
    isDate = isDate(s);

    if (isDate == false) {
      isText = isText(s);
    }

    if (isText == false && isDate == false) {
      isInt = isInt(s);
    }

    if (isInt == true && isDate == false) {
      isZero = isIntZero(s);
    }

    if (isText == false && isDate == false) {
      isDecimal = isDecimal(s);
    }
    
    //isEmpty(s); // this overides the types, need better logic, later...

    if (isDate == true) { // date is first b/c it has text in it
      fieldType = ColumnData.FIELDTYPE_DATETIME; 

    } else if (isText == true && fieldType != ColumnData.FIELDTYPE_DATETIME) {
      fieldType = ColumnData.FIELDTYPE_VARCHAR; 

    } else if (isInt == true && isZero == true && 
        fieldType != ColumnData.FIELDTYPE_DATETIME && 
        fieldType != ColumnData.FIELDTYPE_VARCHAR && 
        fieldType != ColumnData.FIELDTYPE_DECIMAL) { // not date, text, decimal
      fieldType = ColumnData.FIELDTYPE_INT_ZEROFILL; // int with zeros infront 0000123

    } else if (isInt == true && 
        fieldType != ColumnData.FIELDTYPE_VARCHAR && 
        fieldType != ColumnData.FIELDTYPE_TEXT && 
        fieldType != ColumnData.FIELDTYPE_DECIMAL) { // not date,text,decimal
      fieldType = ColumnData.FIELDTYPE_INT;

    } else if (isDecimal == true && 
        fieldType != ColumnData.FIELDTYPE_DATETIME && 
        fieldType != ColumnData.FIELDTYPE_VARCHAR) {
      fieldType = ColumnData.FIELDTYPE_DECIMAL; 

    } else if (isEmpty == true && 
        fieldType != ColumnData.FIELDTYPE_DATETIME && 
        fieldType != ColumnData.FIELDTYPE_VARCHAR && 
        fieldType != ColumnData.FIELDTYPE_INT_ZEROFILL && 
        fieldType != ColumnData.FIELDTYPE_INT && 
        fieldType != ColumnData.FIELDTYPE_DECIMAL) { // has nothing
      fieldType = ColumnData.FIELDTYPE_EMPTY;
      
    } else {
      fieldType = ColumnData.FIELDTYPE_TEXT;
    }

    // debug chain logic
    System.out.println("ThefieldType: " + fieldType + " ForTheValue::: " + s);
  }

  private String getLimitQuery() {

    if (destinationData.optimise_RecordsToExamine <= 0) {
      return "";
    }

    int limit = 0;
    if (destinationData.optimise_RecordsToExamine > 0) {
      limit = destinationData.optimise_RecordsToExamine;
    } 

    String sql = "";
    if (limit > 0) {
      sql = " LIMIT 0," + destinationData.optimise_RecordsToExamine + " ";
    }

    return sql;
  }
  
  private String getColumnType(ColumnData columnData, int charLength) {

    String columnType = null;
    switch (fieldType) {
    case ColumnData.FIELDTYPE_DATETIME: 
      columnType = "DATETIME DEFAULT NULL";
      break;
    case ColumnData.FIELDTYPE_VARCHAR: 
      if (charLength > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + charLength + ") DEFAULT NULL";
      }
      break;
    case ColumnData.FIELDTYPE_INT_ZEROFILL:
      if (charLength <= 8) {
        columnType = "INTEGER(" + charLength + ") ZEROFILL DEFAULT 0";
      } else {
        columnType = "BIGINT(" + charLength + ") ZEROFILL DEFAULT 0";
      }      
      break;
    case ColumnData.FIELDTYPE_INT:
      if (charLength <= 2) {
        columnType = "TINYINT DEFAULT 0";
      } else if (charLength <= 8) {
        columnType = "INTEGER DEFAULT 0";
      } else if (charLength >= 20) { // why am I getting truncation error for 20 bytes?
        columnType = "VARCHAR(" + charLength + ") DEFAULT NULL";
      } else {
        columnType = "BIGINT DEFAULT 0";
      }
      break;
    case ColumnData.FIELDTYPE_DECIMAL:
      columnType = "DECIMAL(" + charLength + "," + decb + ") DEFAULT 0.0";
      break;
    case ColumnData.FIELDTYPE_EMPTY:
      columnType = "CHAR(0)";
      break;
    case ColumnData.FIELDTYPE_TEXT: 
      if (charLength > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + charLength + ") DEFAULT NULL";
      }
      break;
    default:
      if (charLength > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + charLength + ") DEFAULT NULL";
      }
      break;
    }

    return columnType;
  }

  private boolean isEmpty(String s) {
    boolean b = false;
    if (s.isEmpty()) {
      b = true;
    }
    return b;
  }

  private boolean isText(String s) {
    boolean b = false;
    if (s.matches(".*[a-zA-Z].*")) {
      b = true;
    }
    return b;
  }

  private boolean isInt(String s) {
    boolean b = false;
    if (s.matches("[0-9]+")) {
      b = true;
    }
    return b;
  }

  /**
   * is this int with, starts with zeros
   * 
   * @param s
   * @return
   */
  private boolean isIntZero(String s) {
    boolean b = false;
    if (s.matches("[0-9]+") && s.matches("^0[0-9]+")) {
      b = true;
    }
    return b;
  }

  /**
   * is this a decimal?
   * 
   * @param s
   * @return
   */
  private boolean isDecimal(String s) {
    boolean b = false;
    if (s.matches("^\\d+\\.\\d+|\\.\\d+")) {
      b = true;
      getDecimalLengths(s);
    }
    return b;
  }

  private boolean isDate(String s) {
    boolean b = false;
    b = dtp.getIsDateExplicit(s);
    return b;
  }
  
  private void getDecimalLengths(String s) {
    int l = 0;
    int r = 0;
    if (s.contains(".")) {
      String[] a = s.split("\\.");
      l = a[0].length();
      try {
        r = a[1].length();
      } catch (Exception e) {
        r = 0;
      }
      l = l + r;
    } else {
      l = s.length();
    }

    if (l > deca) {
      deca = l;
    }
    
    if (r > decb) {
      decb = r;
    }
    
    System.out.println("decimal: left: " + deca + " right: " + decb + " value: " + s);
  }
  
  private void formatColumn_ToDateTime(ColumnData columnData) {

    ColumnData cpriKey = MySqlTransformUtil.queryPrimaryKey(destinationData.databaseData, columnData.getTable());
    
    String sql = "SELECT " + cpriKey.getColumnName() + ", `" + columnData.getColumnName() + "` " +
    		"FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "`;"; 

    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        cpriKey.setValue(Integer.toString(result.getInt(1)));
        columnData.setValue(result.getString(2));
        updateColumn_Date(cpriKey, columnData);
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }
  
  private void updateColumn_Date(ColumnData cpriKey, ColumnData columnData) {

    String datetime = columnData.getValue();
    
    String tranformed = dtp.getDateMysql(datetime);

    columnData.setValue(tranformed);
    
    System.out.println("before: " + datetime + " after: " + tranformed);

    // is there room for the transformation values
    columnData.alterColumnSizeBiggerIfNeedBe(destinationData.databaseData);
    
    ColumnData[] c = new ColumnData[2];
    c[0] = cpriKey;
    c[1] = columnData;

    String sql = ColumnData.getSql_Update(c);
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }
  
  private void alterColumns() {
    if (alterColumns.size() == 0) {
      return;
    }
    
    ColumnData[] columns = new ColumnData[alterColumns.size()];
    alterColumns.toArray(columns);
    
    MySqlTransformUtil.alterColumn(destinationData.databaseData, columnData);
  }
}
