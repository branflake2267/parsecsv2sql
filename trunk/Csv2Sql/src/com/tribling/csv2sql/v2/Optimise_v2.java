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
 * TODO - delete duplicates - copy from v1
 * TODO - delete empty columns - copy from v1
 * 
 * @author BDonnelson
 * 
 */
public class Optimise_v2 {

  private DestinationData_v2 destinationData = null;
  
  // used to examine the value if its a date, and it can transform it
  private DateTimeParser dtp = new DateTimeParser();
  
  private ColumnData[] columnData = null;

  // resize these columns
  private ArrayList<ColumnData> alterColumns = null;
  
  // when examining the column values, watch the values types
  private int fieldType = ColumnData.FIELDTYPE_TEXT;
  
  // decimal lengths
  private int deca = 0;
  private int decb = 0;
  
  private boolean discoverToTable = false;
  
  public Optimise_v2(DestinationData_v2 destinationData) {
    this.destinationData = destinationData;
  }
  
  /**
   * optimize all the columns
   */
  public void run() {
    
    alterColumns = new ArrayList<ColumnData>();
    
    String where = "`Field` NOT LIKE 'Auto_%'"; // all columns except auto columns
    columnData = MySqlTransformUtil.queryColumns(destinationData.databaseData, destinationData.table, where);
    if (columnData == null) {
      System.out.println("no columns to optimise");
      System.exit(1);
    }
    
    markColumnsThatAreIdents();
    
    processDiscovery();
    
    alterColumns();
  }
    
  /**
   * optimise only these columns
   * 
   * @param columnData
   */
  public void run(ColumnData[] columnData) {
    alterColumns = new ArrayList<ColumnData>();
    
    if (columnData == null) {
      return;
    }
    this.columnData = columnData; 
    
    markColumnsThatAreIdents();
    
    processDiscovery();
    
    alterColumns();
  }
  
  private void markColumnsThatAreIdents() {
    if (destinationData.identityColumns == null) {
      return;
    }
    for (int i=0; i < columnData.length; i++) {
      for (int b=0; b < destinationData.identityColumns.length; b++) {
        if (columnData[i].getColumnName().toLowerCase().equals(destinationData.identityColumns[b].destinationField) == true) {
          columnData[i].setIdentity(true);
        }
      }
    }
  }
  
  /**
   * discover the column types and save them in a tmp table to deal with later
   */
  public void discoverColumnTypes() {
    discoverToTable = true;
    alterColumns = new ArrayList<ColumnData>();
    
    String where = "`Field` NOT LIKE 'Auto_%'"; // all columns except auto columns
    columnData = MySqlTransformUtil.queryColumns(destinationData.databaseData, destinationData.table, where);
    if (columnData == null) {
      System.out.println("no columns to optimise");
      System.exit(1);
    }
    
    markColumnsThatAreIdents();
    
    createTmpDiscoverTable();
    
    processDiscovery();
  }
  
  /**
   * analyze columns to go smaller
   */
  private void processDiscovery() {
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
    
    if (columnData == null) {
      return;
    }
    
    // skip auto_ and primary key
    if (columnData.getColumnName().matches("Auto_.*") == true) {
      return;
    }
    
    if (columnData.getIsPrimaryKey() == true) {
      return;
    }
    
    // only analyze text columns
    if (destinationData.optimise_TextOnlyColumnTypes == true && 
        columnData.getType().toLowerCase().contains("text") == false) {
      System.out.println("checkColumn(): skipping b/c destinationData.optimise_TextOnlyColumnTypes=true and columnData.getType().toLowerCase().contains(\"text\") = false");
      return;
    }
      
    // examine for the best field type (column type) int, decimal, text varchar.
    analyzeColumnType(columnData);
    
    // get character max length in column
    int maxCharLength = getMaxLength(columnData);
    
    String newColumnType = getColumnType(columnData, maxCharLength);
    
    // did type change, then alter
    boolean changed = didItChange(columnData, newColumnType);
    
    // TODO - if changing to a datetime, need to transform all values in the column to datetime
    
    if (changed == true) {
      alter(columnData, newColumnType);
    }
    
    if (discoverToTable == true) {
      saveToTmpTable(columnData, newColumnType, maxCharLength, changed, fieldType, deca, decb);
    }
  }
  
  private void saveToTmpTable(ColumnData columnData, String newColumnType,
      int maxCharLength, boolean changed, int fieldType, int deca, int decb) {
    
    String tmptable = destinationData.table + "_auto_discover";
    String sql = "INSERT INTO " + tmptable + " SET " +
    		"DateCreated=NOW(), " +
    		"Column_Name='" + columnData.getColumnName() + "', " +
    		"Column_Len='" + columnData.getCharLength() + "', " +
    		"ColumnType_New='" + newColumnType + "', " +
    		"MaxCharLen='" + maxCharLength + "', " +
    		"FieldType='" + fieldType + "', " +
    		"DecA='" + deca + "', " +
    		"DecB='" + decb + "';";
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }

  private void createTmpDiscoverTable() {
    String tmptable = destinationData.table + "_auto_discover";
    MySqlTransformUtil.createTable(destinationData.databaseData, tmptable, "Id");
    
    ColumnData c0 = new ColumnData(tmptable, "DateCreated", "DATETIME");
    ColumnData c1 = new ColumnData(tmptable, "Column_Name", "VARCHAR(50)");
    ColumnData c2 = new ColumnData(tmptable, "Column_Len", "INTEGER");
    ColumnData c3 = new ColumnData(tmptable, "ColumnType_New", "VARCHAR(100)");
    ColumnData c4 = new ColumnData(tmptable, "MaxCharLen", "INTEGER");
    ColumnData c5 = new ColumnData(tmptable, "FieldType", "INTEGER");
    ColumnData c6 = new ColumnData(tmptable, "DecA", "INTEGER");
    ColumnData c7 = new ColumnData(tmptable, "DecB", "INTEGER");
    MySqlTransformUtil.createColumn(destinationData.databaseData, c0);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c1);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c2);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c3);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c4);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c5);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c6);
    MySqlTransformUtil.createColumn(destinationData.databaseData, c7);
  }
  
  private int getMaxLength(ColumnData columnData) {
    String sql = ColumnData.getSql_GetMaxCharLength(destinationData.databaseData, columnData);
    System.out.println("checking column length: " + sql);
    int maxCharLength = MySqlQueryUtil.queryInteger(destinationData.databaseData, sql);
    return maxCharLength;
  }
  
  public void alterExplicit(ColumnData columnData, String columnType) {
    alterColumns = new ArrayList<ColumnData>();
    
    alter(columnData, columnType);
    
    alterColumns();
  }
  
  private void alter(ColumnData columnData, String columnType) {
    
    boolean isPrimKey = MySqlTransformUtil.queryIsColumnPrimarykey(destinationData.databaseData, columnData);
    if (isPrimKey == true) {
      destinationData.debug("alter(): skipping altering primary key: " + columnData.getColumnName());
      return;
    }
    
    // when altering dates, make sure every value is transformed to
    if (columnType.toLowerCase().contains("datetime") == true) {
      formatColumn_ToDateTime(columnData);
    } else if (columnType.toLowerCase().contains("int") == true) {
      formatColumn_ToInt(columnData);
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
    
    if (columnData.getType().contains("text") == false) {
      System.out.println("analyzeColumnType(): Type already defined in columnData, skipping and going with column definition.");
      return;
    }
    
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
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
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

    if (isDate == true && isText == false && isInt == false && isDecimal == false && 
        fieldType != ColumnData.FIELDTYPE_VARCHAR &&
        fieldType != ColumnData.FIELDTYPE_INT &&
        fieldType != ColumnData.FIELDTYPE_INT_ZEROFILL &&
        fieldType != ColumnData.FIELDTYPE_TEXT &&
        fieldType != ColumnData.FIELDTYPE_DECIMAL) { // date is first b/c it has text in it
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
        fieldType != ColumnData.FIELDTYPE_DECIMAL &&
        fieldType != ColumnData.FIELDTYPE_INT_ZEROFILL) { // not date,text,decimal
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
      //fieldType = ColumnData.FIELDTYPE_TEXT;
    }

    // debug chain logic
    destinationData.debug("ThefieldType: " + fieldType + " ForTheValue::: " + s + " isText:"+ isText + " isInt:" + isInt + " isZeroInt:"+isZero + " isDecimal:"+isDecimal);
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

    if (columnData.getType().contains("text") == false) {
      System.out.println("getColumnType(): column's type is already set. Skipping setting a new one.");
      return columnData.getType();
    }
    
    // can skip discovery of other types
    if (destinationData.skipOptimisingIntDateTimeDecTypeColumns == true) {
      fieldType = 2;
    }
    
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
        columnType = "INTEGER(" + charLength + ") ZEROFILL  DEFAULT 0"; 
      } else {
        columnType = "BIGINT(" + charLength + ") ZEROFILL DEFAULT 0"; 
      }      
      break;
    case ColumnData.FIELDTYPE_INT:
      if (charLength <= 2) {
        columnType = "TINYINT"; // DEFAULT 0
      } else if (charLength <= 8) {
        columnType = "INTEGER"; // DEFAULT 0
      } else if (charLength >= 20) { // why am I getting truncation error for 20 bytes?
        columnType = "VARCHAR(" + charLength + ") DEFAULT NULL";
      } else {
        columnType = "BIGINT"; // DEFAULT 0
      }
      break;
    case ColumnData.FIELDTYPE_DECIMAL:
      columnType = "DECIMAL(" + charLength + "," + decb + ")"; // not doing this b/c it errors DEFAULT 0.0 when nothing exists ''
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
    if (s.matches("[-]?[0-9]+")) {
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
    if (s.matches("^[-]?\\.\\d+|^[-]?\\d+\\.\\d+")) {
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
    
    destinationData.debug("decimal: left: " + deca + " right: " + decb + " value: " + s);
  }
  
  private void formatColumn_ToDateTime(ColumnData columnData) {
    String sql = "SELECT COUNT(*) AS t FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "`;"; 
    long total = MySqlQueryUtil.queryLong(destinationData.databaseData, sql);
    
    int lim = 1000;
    int totalPages = (int) (total / lim);

    int offset = 0;
    int limit = 0;
    for (int i=0; i < totalPages; i++) {
      if (i==0) {
        offset = 0;
        limit = 1000;
      } else {
        offset = ((i+1)*1000) - 1000;
        limit = ((i+1)*1000);
      }
      
      formatColumn_ToDateTime(columnData, offset, limit);
    }
    
  }
  
  private void formatColumn_ToDateTime(ColumnData columnData, int offset, int limit) {
    
    ColumnData cpriKey = MySqlTransformUtil.queryPrimaryKey(destinationData.databaseData, columnData.getTable());
    
    String sql = "SELECT " + cpriKey.getColumnName() + ", `" + columnData.getColumnName() + "` " +
    		"FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "` LIMIT " + offset + ", " + limit + ";"; 
    System.out.println(sql);
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
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

    if (datetime == null) {
      tranformed = null;
    } else if (datetime.trim().length() == 0) {
      tranformed = null;
    } 
    
    columnData.setValue(tranformed);
    destinationData.debug("column: " + columnData.getColumnName() + " datetime before: " + datetime + " after: " + tranformed);

    // is there room for the transformation values
    columnData.alterColumnSizeBiggerIfNeedBe(destinationData.databaseData);
    
    ColumnData[] c = new ColumnData[2];
    c[0] = cpriKey;
    c[1] = columnData;

    String sql = ColumnData.getSql_Update(c);
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }
  
  private void formatColumn_ToInt(ColumnData columnData) {

    String sql = "SELECT COUNT(*) as t FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "`;";
    long total = MySqlQueryUtil.queryLong(destinationData.databaseData, sql);
    
    int lim = 1000;
    int totalPages = (int) (total / lim);

    int offset = 0;
    int limit = 0;
    for (int i=0; i < totalPages; i++) {
      if (i==0) {
        offset = 0;
        limit = 1000;
      } else {
        offset = ((i+1)*1000) - 1000;
        limit = ((i+1)*1000);
      }
      
      formatColumn_ToInt(columnData, offset, limit);
    }
    
  }
  
  private void formatColumn_ToInt(ColumnData columnData, int offset, int limit) {

    ColumnData cpriKey = MySqlTransformUtil.queryPrimaryKey(destinationData.databaseData, columnData.getTable());
    
    String sql = "SELECT " + cpriKey.getColumnName() + ", `" + columnData.getColumnName() + "` " +
        "FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "` LIMIT " + offset + ", " + limit + ";"; 
    
    System.out.println(sql);
    
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        cpriKey.setValue(Integer.toString(result.getInt(1)));
        columnData.setValue(result.getString(2));
        updateColumn_Int(cpriKey, columnData);
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }
  
  private void updateColumn_Int(ColumnData cpriKey, ColumnData columnData) {

    String intvalue = columnData.getValue();
    
    String tranformed = columnData.getValue();
    if (intvalue == null) {
      tranformed = "0";
    } else if (intvalue.trim().length() == 0) {
      tranformed = "0";
    } else {
      try {
        Integer.parseInt(tranformed);
      } catch (NumberFormatException e) {
        tranformed = "0";
      } 
    }

    columnData.setValue(tranformed);
    
    destinationData.debug("column: " + columnData.getColumnName() + " int before: " + intvalue + " after: " + tranformed);

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
    
    MySqlTransformUtil.alterColumn(destinationData.databaseData, columns);
  }
  
  /**
   * how many duplicates are in table?
   * 
   * @return
   */
  public long getTableHasDuplicates() {
    
    // get total record count for table
    long tc = getTableRecordCount();
    
    // check distinct count for identities
    long tdc = getTableDistinctIdentCount();
    
    long r = tc - tdc;
    
    return r;
  }
  
  private long getTableRecordCount() {
    String sql = "SELECT COUNT(*) AS t FROM " + destinationData.databaseData.getDatabase() + "." + destinationData.table + ";";
    System.out.println(sql);
    return MySqlQueryUtil.queryLong(destinationData.databaseData, sql);
  }
  
  private long getTableDistinctIdentCount() {
    
    if (destinationData.identityColumns == null) {
      return 0;
    }
    
    // get ident columns
    String idents_Columns = getIdentitiesColumns_inCsv();
    
    String sql = "SELECT DISTINCT " + idents_Columns + " FROM " + 
      destinationData.databaseData.getDatabase() + "." + destinationData.table + ";"; 

    System.out.println(sql);
    
    long c = 0;
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
      ResultSet result = select.executeQuery(sql);
      c = MySqlQueryUtil.getResultSetSize(result);
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    return c;
  }
  
  private String getIdentitiesColumns_inCsv() {
    if (destinationData.identityColumns == null) {
      return "";
    }
    String columns = "";
    for (int i = 0; i < destinationData.identityColumns.length; i++) {
      columns += destinationData.identityColumns[i].destinationField;
      if (i < destinationData.identityColumns.length - 1) {
        columns += ",";
      }
    }
    
    return columns;
  }
  
  public void deleteDuplicates() {
    
    long c = getTableHasDuplicates();
    
    if (c == 0) {
     System.out.println("No duplicates exist for the identities.");
     return;
    }
    
    String idents_Columns = getIdentitiesColumns_inCsv();
    
    // load the records that indicate they there duplicates
    String sql = "SELECT " + destinationData.primaryKeyName + " FROM " + 
    destinationData.databaseData.getDatabase() + "." + destinationData.table + " " +
    		"GROUP BY "+ idents_Columns + " HAVING count(*) > 1;"; 

    System.out.println(sql);
    
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
      ResultSet result = select.executeQuery(sql);
      int index = MySqlQueryUtil.getResultSetSize(result);
      while (result.next()) {
        processDuplicate(index, result.getInt(1));
        index--;
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }
  
  private void processDuplicate(int index, int uniqueId) {
    
    String idents_Columns = getIdentitiesColumns_inCsv();
    String where = "WHERE " + destinationData.primaryKeyName + "='" + uniqueId + "'";
    
    String sql = "SELECT "+ idents_Columns + " FROM " + 
      destinationData.databaseData.getDatabase() + "." + destinationData.table + " " + where; 

    System.out.println(index + ". " + sql);
    
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        String[] values = new String[destinationData.identityColumns.length];
        for (int i=0; i < destinationData.identityColumns.length; i++) {
          values[i] = result.getString(i+1);
        }
        deleteDuplicate(values);
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }

  private void deleteDuplicate(String[] identValues) {
    
    String where = "";
    for (int i=0; i < destinationData.identityColumns.length; i++) {
      where += "" + destinationData.identityColumns[i].destinationField + "='" + identValues[i] + "'";
      if (i < destinationData.identityColumns.length-1) {
        where += " AND ";
      }
    }
    
    String sql = "SELECT " + destinationData.primaryKeyName + " FROM " + 
    destinationData.databaseData.getDatabase() + "." + destinationData.table + " WHERE " + where; 
 
    System.out.println("sql" + sql);
    
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
          java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(Integer.MIN_VALUE);
      ResultSet result = select.executeQuery(sql);
      int i = 0;
      while (result.next()) {
        int uniqueId = result.getInt(1);
        if (i > 0) {
          deleteRecord(uniqueId);
        }
        i++;
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }
  
  private void deleteRecord(int uniqueId) {
    
    String where = "" + destinationData.primaryKeyName + "='" + uniqueId + "'";
    
    String sql = "DELETE FROM " + destinationData.databaseData.getDatabase() + "." + destinationData.table + " WHERE " + where; 
    
    System.out.println("sql: " + sql);
    
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }
  
}
