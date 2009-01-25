package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.data.ColumnData;

/**
 * 
 * TODO - add an option to skip non TEXT type columns, this way, not to optimize anything already possibily optimized, 
 *        optimise can always make it bigger automatically
 * 
 * @author BDonnelson
 * 
 */
public class Optimise extends SQLProcessing {

  
  private int fieldType = 0;
  
  private int fieldLength = 0;

  // for progressive alter column tries
  // try more than once, in case it throws a truncated exception
  private int alterTries = 0;
  private int alterTries_UpRecordSampleCount = 0;
  
  // track sampling of record analyziation
  private int analyzeTrackingNextNotfication = 500;
  private int analyzeTracking = 0;
  
  /**
   * constructor
   */
  public Optimise() {
  }

  protected void runOptimise(ColumnData[] columns) {

    if (dd.optimise == false) {
      System.out.println("skipping optimising: destinationData.optimise = false");
      return;
    }

    // loop through each column
    loopThroughColumns(columns);

    System.out.println("Done Optimising");
  }

  /**
   * optimise the table columns/fields to the lengths that are best
   */
  public void runOptimise() {

    if (dd.optimise == false) {
      System.out.println("skipping optimising: destinationData.optimise = false");
      return;
    }

    // open connections to work with the data
    openConnection();

    // have to delete all indexes except primary key so to fix the indexed columns
    deleteIndexesBeforeOptimisation();
    
    // do this first, b/c it will cause problems after getting columns
    deleteEmptyColumns();

    // get columns in the table
    ColumnData[] columns = getColumns();

    // go fix the columns up - make em better
    columns = fixColumns(columns);

    // loop through each column
    loopThroughColumns(columns);

    // close the connections at the end
    closeConnection();
  }

  /**
   * delete all the indexes, then optimise all columns
   * Note: not able to alter a indexed column
   */
  private void deleteIndexesBeforeOptimisation() {
    
    if (dd.skipDeletingIndexingBeforeOptimise == true) {
      return;
    }
    
    // delete all indexes, so columns can be optimised, then re-index
    deleteAllIndexs();
    
  }
  
  /**
   * resize a column
   * 
   * @param column
   * @param columnType
   * @param length
   */
  public String resizeColumn(String column, String columnType, int length) {
    this.fieldLength = length;
    this.fieldType = getFieldType(columnType);

    String type = "";
    if (databaseType == 1) {
      type = getColumnType_MySql();
    } else if (databaseType == 2) {
      type = getColumnType_MsSql();
    }

    alterColumn(column, type);
    
    return type;
  }

  private void loopThroughColumns(ColumnData[] columns) {

    int i2 = columns.length - 1;
    for (int i = 0; i < columns.length; i++) {

      // reset
      fieldType = 0;
      fieldLength = 0;

      // console
      System.out.println(i2 + ". Analyzing column: " + columns[i].column);

      // analyze column
      analyzeColumn(columns[i].column);

      // get type that was determined by analyzeColumn
      String columnType = getColumnType();

      // alter column
      alterColumn(columns[i].column, columnType);

      i2--;
    }

  }

  /**
   * alter column
   * 
   * MSSQL: TODO - when altering a column that has an index attatched to it
   * errors - force it, or is anohter way maybe to alter it when has index on it
   * 
   * TODO - on altering a column and not sampling enough records, can cause an
   * exception need to analyze all and try agian maybe
   * TODO - resample larger set of records on failure
   * 
   * @param column
   * @param columnType
   */
  private void alterColumn(String column, String columnType) {

    // skip system columns
    if (column.equals("ImportID") | column.equals("DateCreated") | column.equals("DateUpdated")) {
      return;
    }
    
    // does column already have this type? skip if it does
    ColumnData compareColumn = getColumn(column);
    if (columnType.toLowerCase().contains(compareColumn.type.toLowerCase())) {
      System.out.println("Column already has this columnType: " + compareColumn.type);
      return;
    }

    String modifyColumn = "";
    String alterQuery = "";
    if (databaseType == 1) {
      modifyColumn = "`" + column + "` " + columnType;
      alterQuery = "ALTER TABLE `" + dd.database + "`.`" + dd.table
          + "` MODIFY COLUMN " + modifyColumn;
      
    } else if (databaseType == 2) {
      modifyColumn = "[" + column + "] " + columnType;
      alterQuery = "ALTER TABLE " + dd.database + "." + dd.tableSchema + "."
          + dd.table + " ALTER COLUMN " + modifyColumn;
    }

    // TODO - resample larger amount of records on exception
    // TODO - then can start with smaller sample size
    try {
      Connection conn = getConnection();
      Statement update = conn.createStatement();
      update.executeUpdate(alterQuery);
      update.close();
      
    } catch (SQLException e) {
      System.err.println("Alter failure: " + alterQuery);
      e.printStackTrace();
      System.out.println("");

      alterColumnTryAgain(e, column);
    }

    // if we make it this far, make sure these get reset
    alterTries = 0; // reset
    alterTries_UpRecordSampleCount = 0; // reset
    
  }
  
  /**
   * try altering again, after larger sampling
   * 
   * NOTE could use the row that got truncated.
   * 
   * @param e
   * @param column
   */
  private void alterColumnTryAgain(SQLException e, String column) {
    
    // TODO try up to 8 times??
    if (alterTries == 7) {
      alterTries = 0; // reset
      alterTries_UpRecordSampleCount = 0; // reset
      System.out.println("Won't try the alter agian, moving on");
      return;
    }
    
    if (alterTries == 0) {
      alterTries_UpRecordSampleCount = dd.optimiseRecordsToExamine;
    } else {
      alterTries_UpRecordSampleCount = dd.optimiseRecordsToExamine * 2; 
    }
    
    analyzeColumn(column);
    
    alterTries++;
  }
  
  
  private String getLimitQuery() {

    // when this is set to 0 sample all
    if (dd.optimiseRecordsToExamine <= 0) {
      return "";
    }

    int limit = 0;
    if (dd.optimiseRecordsToExamine > 0) {
      limit = dd.optimiseRecordsToExamine;
    } else if (alterTries_UpRecordSampleCount > 0) { // on a retry up the sampling count
      limit = alterTries_UpRecordSampleCount;
    }

    String sql = "";
    if (limit > 0) {
      if (databaseType == 1) {
        sql = " LIMIT 0," + dd.optimiseRecordsToExamine + " ";
      } else if (databaseType == 2) {
        sql = " TOP " + dd.optimiseRecordsToExamine + " ";
      }
    }

    return sql;
  }

  /**
   * analyze a column for its column type and length 
   *    like varchar(50)
   * 
   * @param column
   */
  private void analyzeColumn(String column) {

    // notify every so many
    analyzeTrackingNextNotfication = 0;
    
    if (column.equals("ImportID")) {
      System.out.println("Skipping ImportID");
      return;
    }
    
    // If a sampling number is set, sample radomly
    // this helps in large record sets
    String random = "";
    if (dd.optimiseRecordsToExamine > 0) {
      if (databaseType == 1) {
        random = "ORDER BY RAND()";
      } else if (databaseType == 2) {
        random = "ORDER BY NEWID()";
      }
    }

    String query = "";
    if (databaseType == 1) {
      query = "SELECT " + column + " " + 
      "FROM " + dd.database + "." + dd.table + " " +
      		"WHERE (" + column + " IS NOT NULL) " + random + " " + getLimitQuery() + ";"; 
      
    } else if (databaseType == 2) {
      query = "SELECT " + getLimitQuery() + " " + column + " " + 
      "FROM " + dd.database + "." + dd.tableSchema + 
      "." + dd.table + " (" + column + " IS NOT NULL) " + random + ";"; 
    }

    System.out.println("Analyzing Column For Type: " + column + " query: " + query);

    try { 
      // only read 500 records at a time, so not use a ton of memory for large samples
      Connection conn = getConnection();
      Statement select = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
      select.setFetchSize(500); 
      ResultSet result = select.executeQuery(query);
      int i = 0;
      while (result.next()) {
        
        // draw to screen where we are at every so often
        trackAnalyzation(i);
        
        String s = result.getString(1);
        examineField(s);
        
        i++;
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("SQL Statement Error:" + query);
      e.printStackTrace();
    }
  }
  
  private void examineField(String s) {

    if (s == null) {
      s = "";
    }

    // whats the size of the field length
    setFieldLength(s);

    // NOTE: optimise only the varchar/text fields
    // (this has to be done first anyway so we can alter varchar->date
    // varchar->int...)
    if (dd.optimiseTextOnly == true) {
      fieldType = 2;
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

    if (isText == false) {
      isInt = isInt(s);
    }

    if (isInt == true) {
      isZero = isIntZero(s);
    }

    if (isText == false) {
      isDecimal = isDecimal(s);
    }

    isEmpty = isEmpty(s);

    if (isDate == true) { // date is first b/c it has text in it
      fieldType = 1; // date

    } else if (isText == true && fieldType != 1) {
      fieldType = 2; // varchar

    } else if (isInt == true && isZero == true && fieldType != 1
        && fieldType != 2 && fieldType != 5) { // not date, text, decimal
      fieldType = 3; // int with zeros infront 0000123

    } else if (isInt == true && fieldType != 2 && fieldType != 3
        && fieldType != 5) { // not date,text,decimal
      fieldType = 4; // is Int

    } else if (isDecimal == true && fieldType != 1 && fieldType != 2) {
      fieldType = 5; // is decimal

    } else if (isEmpty == true && fieldType != 1 && fieldType != 2
        && fieldType != 3 && fieldType != 4 && fieldType != 5) { // has nothing
      fieldType = 6;
    } else {
      fieldType = 7;
    }

    System.out.println("fieldType: " + fieldType + " Length: " + fieldLength
        + " Value::: " + s);
  }

  // TODO - hmmmm can't what I was doing with the types
  //final static int FIELDTYPE_TEXT = 2;
  //final static int FIELDTYPE_DATE = 1;
  //final static int FIELDTYPE_VARCHAR = 2;
  
  /**
   * change the field type int a Int reference
   * 
   * TODO - change field types int static final vars
   * 
   * @param columnType
   * @return
   */
  private int getFieldType(String columnType) {

    String type = columnType.toLowerCase();

    int fieldType = 0;
    if (type.contains("text")) {
      fieldType = 2;
    } else if (type.contains("date")) {
      fieldType = 1;
    } else if (type.contains("varchar")) {
      fieldType = 2;
    } else if (type.contains("int")) {
      fieldType = 3;
    } else if (type.contains("decimal")) {
      fieldType = 5;
    } else if (type.length() == 0) {
      fieldType = 6;
    }

    return fieldType;
  }

  /**
   * get ready to figure out how to alter the column
   */
  private String getColumnType() {

    String columnType = null;

    if (databaseType == 1) {
      columnType = getColumnType_MySql();
    } else if (databaseType == 2) {
      columnType = getColumnType_MsSql();
    }

    return columnType;
  }

  private String getColumnType_MySql() {

    int len = getLenthForType();

    if (len == 0) {
      len = 1;
    }

    String columnType = "";
    switch (fieldType) {
    case 1: // datetime
      columnType = "DATETIME DEFAULT NULL";
      break;
    case 2: // varchar
      if (len > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + len + ") DEFAULT NULL";
      }
      break;
    case 3: // int unsigned - with zero fill
      columnType = "INT DEFAULT NULL";
      break;
    case 4: // int
      // TODO small, medium ints?
      if (len < 8) {
        columnType = "INT UNSIGNED ZEROFILL DEFAULT " + len;
      } else {
        columnType = "BIGINT UNSIGNED ZEROFILL DEFAULT " + len;
      }
      break;
    case 5: // decimal
      // TODO columnType = "DECIMAL(18, 2) DEFAULT NULL";
      columnType = "VARCHAR(50) DEFAULT NULL";
      break;
    case 6: // empty
      columnType = "VARCHAR(" + len + ") DEFAULT NULL";
      break;
    case 7: // other
      columnType = "VARCHAR(" + len + ") DEFAULT NULL";
      break;
    default:
      if (len > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + len + ") DEFAULT NULL";
      }
      break;
    }

    return columnType;
  }

  private String getColumnType_MsSql() {

    int len = getLenthForType();

    if (len == 0) {
      len = 1;
    }

    String columnType = "";
    switch (fieldType) {
    case 1: // datetime
      columnType = "[DATETIME] NULL";
      break;
    case 2: // varchar
      if (len > 255) {
        columnType = "TEXT NULL";
      } else {
        columnType = "VARCHAR(" + len + ") NULL";
      }
      break;
    case 3: // int unsigned - with zero fill
      // TODO ?? for zero fill?
      columnType = "[INT] NULL";
      break;
    case 4: // int
      if (len < 8) {
        columnType = "[INT] NULL";
      } else {
        columnType = "[BIGINT] NULL";
      }
      break;
    case 5: // decimal
      // TODO - [decimal](18, 0) NULL
      columnType = "[VARCHAR](50) NULL";
      break;
    case 6: // empty
      columnType = "[VARCHAR](" + len + ") NULL"; // TODO - delete this column
                                                  // later
      break;
    case 7: // other
      columnType = "[VARCHAR](" + len + ") NULL"; // TODO - delete this column
                                                  // later
      break;
    default:
      if (len > 255) {
        columnType = "TEXT DEFAULT NULL";
      } else {
        columnType = "VARCHAR(" + len + ") NULL";
      }
      break;
    }

    return columnType;
  }

  /**
   * optimise length of a particular column Type
   * 
   * TODO - figure out decimal, watch for before and after . during parse TODO -
   * do I want to make a percenatage bigger than needed? like make bigger * 10%
   * although, for now I think I will skip this, make it exact, seems to work
   * 
   * @return
   */
  private int getLenthForType() {

    int l = 0;

    switch (fieldType) {
    case 1: // datetime
      l = fieldLength;
      break;
    case 2: // varchar
      l = fieldLength;
      break;
    case 3: // int unsigned - with zero fill
      l = fieldLength;
      break;
    case 4: // int
      l = fieldLength;
      break;
    case 5: // decimal
      l = fieldLength;
      break;
    case 6: // empty
      l = fieldLength;
      break;
    case 7: // other
      l = fieldLength;
      break;
    default:
      l = fieldLength;
      break;
    }

    return l;
  }

  private void setFieldLength(String s) {
    int size = s.length();
    if (size > fieldLength) {
      fieldLength = size;
    }
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
   * starts with zeros
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

  private boolean isDecimal(String s) {
    boolean b = false;
    if (s.matches("^\\d+\\.\\d+|\\.\\d+")) {
      b = true;
    }
    return b;
  }

  /**
   * add more date identifications
   * 
   * TODO -> jan 09 -> transform it too.
   * TODO - transform date into epoch int
   * 
   * @param s
   * @return
   */
  private boolean isDate(String s) {

    s = s.toLowerCase();

    if (s.length() == 0) {
      return false;
    }

    boolean b = false;

    if (s.contains("jan")) {
      b = true;
    } else if (s.contains("feb")) {
      b = true;
    } else if (s.contains("feb")) {
      b = true;
    } else if (s.contains("mar")) {
      b = true;
    } else if (s.contains("apr")) {
      b = true;
    } else if (s.contains("may")) {
      b = true;
    } else if (s.contains("jun")) {
      b = true;
    } else if (s.contains("jul")) {
      b = true;
    } else if (s.contains("aug")) {
      b = true;
    } else if (s.contains("sep")) {
      b = true;
    } else if (s.contains("oct")) {
      b = true;
    } else if (s.contains("nov")) {
      b = true;
    } else if (s.contains("dec")) {
      b = true;
    }

    // TODO - proof this later
    if (s.matches("[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4}.*")) {
      b = true;
    }

    return b;
  }
  
  /**
   * draw to screen every so often where we are at in the record set
   * @param i
   */
  private void trackAnalyzation(int i) {
    
    if (i == analyzeTracking) {
      System.out.println(i);
      analyzeTracking = analyzeTracking + analyzeTrackingNextNotfication;
    }
    
  }

  
  
  
  
}
