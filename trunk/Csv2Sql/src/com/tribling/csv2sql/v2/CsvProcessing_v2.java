package com.tribling.csv2sql.v2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import com.csvreader.CsvReader;
import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.FieldData;
import com.tribling.csv2sql.data.FieldDataComparator;
import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class CsvProcessing_v2 extends FlatFileProcessing_v2 {

  // csv reader 2.0
  private CsvReader reader = null;

  // work on this file
  private File file = null;
  
  // data source
  private SourceData sourceData = null;
  
  // data destination
  private DestinationData_v2 destinationData = null;

  // columns being imported 
  private ColumnData[] columnData = null;
  
  private ColumnData[] defaultColumns = null;
  
  /**
   * constructor
   */
  public CsvProcessing_v2(SourceData sourceData, DestinationData_v2 destinationData) {
    this.sourceData = sourceData;
    this.destinationData = destinationData;
  }
  
  /**
   * parse the csv and insert it into sql
   * 
   * @param fileIndex
   * @param file
   */
  protected void parseFile(int fileIndex, File file) {
    this.file = file;

    // open the csv file for reading
    openFileAndRead();

    // create table
    createTable();
    
    // create default columns datecreated, dateupdated
    createDefaultFields();
    
    // get and create columns
    createColumns();

    createIdentitiesIndex();
    
    // loop through data rows
    iterateRowsData(fileIndex);

  }
  
  private void createIdentitiesIndex() {
    if (destinationData.identityColumns == null) {
      return;
    }
   
    String sql = ColumnData.getSql_IdentitiesIndex(destinationData.databaseData, columnData);
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }

  /**
   * look for match only
   * 
   * @param fileIndex
   * @param file
   * @return
   */
  protected boolean parseFile_Match(int fileIndex, File file) {
    this.file = file;

    // open the csv file for reading
    openFileAndRead();

    // TODO - finish
    
    return false;
  }

  /**
   * open file and start reading it
   */
  private void openFileAndRead() {
    
    try {
      if (Character.toString(sourceData.delimiter) == null) {
        System.out.println("openFileAndRead: You forgot to set a delimiter. Exiting.");
        System.exit(1);
      }
    } catch (Exception e1) {
      System.out.println("openFileAndRead: You forgot to set a delimiter. Exiting.");
      e1.printStackTrace();
      System.exit(1);
    }
    
    try {     
      reader = new CsvReader(file.toString(), sourceData.delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("CSV Reader, Could not open CSV Reader");
      e.printStackTrace();
    }
  }
  
  /**
   * create columns from csv
   */
  private void createColumns() {
    columnData = getColumnsFromCsv();
    MySqlTransformUtil.createColumn(destinationData.databaseData, columnData);
  }
  
  /**
   * create default columns
   */
  private void createDefaultFields() {
    defaultColumns = new ColumnData[2];
    defaultColumns[0] = new ColumnData();
    defaultColumns[1] = new ColumnData();
    
    defaultColumns[0].setTable(destinationData.table);
    defaultColumns[0].setColumnName("Auto_DateCreated");
    defaultColumns[0].setType("DATETIME DEFAULT NULL");
    defaultColumns[0].setValueAsFunction("NOW()");
   
    defaultColumns[1].setTable(destinationData.table);
    defaultColumns[1].setColumnName("Auto_DateUpdated");
    defaultColumns[1].setType("DATETIME DEFAULT NULL");
    defaultColumns[1].setValueAsFunction("NOW()");
    
    MySqlTransformUtil.createColumn(destinationData.databaseData, defaultColumns);
  }

  /**
   * get columns from csv
   * 
   * @return
   */
  private ColumnData[] getColumnsFromCsv() {
    ColumnData[] columnData = null;	
    try {
      reader.readHeaders();
      columnData = createColumnsFromCsvHeader(reader.getHeaders());
    } catch (IOException e) {
      System.out.println("couln't read columns");
      e.printStackTrace();
    }
    return columnData;
  }
  
  /**
   * make a column 1(header) list of the fields, 
   *   which will always be needed when inserting/updating the data into sql
   * 
   *   ~~~ first row processsing ~~~
   * 
   * @param header
   * @return
   */
  private ColumnData[] createColumnsFromCsvHeader(String[] header) {
    ColumnData[] columnData = new ColumnData[header.length];
    for(int i=0; i < header.length; i++) {
      columnData[i] = new ColumnData();
      columnData[i].setTable(destinationData.table);
     
      // is this column used for identity
      if (isThisColumnUsedForIdentity(columnData[i], header[i])) {
        columnData[i].setIdentity(true);
      }
      
      // pre-process flat file values
      header[i] = evaluate(0, i, header[i]);
      
      // change Column Name by using Field matching
      header[i] = changeColumnName(header[i]);
      
      if (destinationData != null && destinationData.firstRowHasNoFieldNames == true && i < header.length - 1) { 
        columnData[i].setColumnName(" "); // this will change into 'c0','c1',... column names
      } else {
        columnData[i].setColumnName(header[i]);
      }
      
    }
    return columnData;
  }
  
  /**
   * is this Column used for identity
   * 
   * TODO - deal with no identities being used
   * 
   * @param columnData
   * @param headerValue
   * @return
   */
  private boolean isThisColumnUsedForIdentity(ColumnData columnData, String headerValue) {
    // find the destination field by comparing source field
    int index = FieldData.getSourceFieldIndex(destinationData.identityColumns, headerValue);
    
    // no matches found in field data
    if (index < 0) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * change the column header name by field matching
   * 
   * @param columnName
   * @return columnName
   */
  private String changeColumnName(String columnName) {
    if (destinationData.changeColumn == null) {
      return columnName;
    }
    Comparator<FieldData> searchByComparator = new FieldDataComparator();
    FieldData searchFor = new FieldData();
    searchFor.sourceField = columnName;
    int index = Arrays.binarySearch(destinationData.changeColumn, searchFor, searchByComparator);
    if (index >= 0) {
      columnName = destinationData.changeColumn[index].destinationField;
    }
    return columnName;
  }
  
  /**
   * iterate through data in file
   * 
   * @param indexFile
   */
  private void iterateRowsData(int indexFile) {
    int index = 0;
    
    // when the first row is data, need to move up one row to start
    if (destinationData != null && destinationData.firstRowHasNoFieldNames == true) {
      index--;
    }
    
    try {
      while (reader.readRecord()) {
        process(index, reader);
        index++;
      }
    } catch (IOException e) {
      System.out.println("Error: Can't loop through data!");
      e.printStackTrace();
    }
  }
  
  /**
   * process values from csv row
   * 
   * @param index
   * @param reader
   */
  private void process(int index, CsvReader reader) {
    String[] values = null;
    
    // get values from row
    try {
      values = reader.getValues();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    // pre-process values in row
    values = preProcessFlatFileValues(index + 1, values);
    
    // add values to columnData
    columnData = ColumnData.addValues(columnData, values);
    
    testColumnValueSizes();
    
    // save Columns/Values to db
    save();
  }
  
  private void testColumnValueSizes() {
    for (int i=0; i < columnData.length; i++) {
      columnData[i].alterColumnSizeBiggerIfNeedBe(destinationData.databaseData);
    }
  }

  /**
   * save the column's values into db
   */
  private void save() {
    
    long primaryKeyId = doesDataExist();
     
    String sql = "";
    if (primaryKeyId == 0) {
      ColumnData[] i = ColumnData.merge(columnData, defaultColumns[0]); // add insert column
      sql = ColumnData.getSql_Insert(i);
    } else { 
      ColumnData primaryKeyColumn = new ColumnData();
      primaryKeyColumn.setIsPrimaryKey(true);
      primaryKeyColumn.setColumnName(destinationData.primaryKeyName);
      primaryKeyColumn.setValue(primaryKeyId);
      ColumnData[] u = ColumnData.merge(columnData, defaultColumns[1]); // add update column
      u = ColumnData.merge(u, primaryKeyColumn);
      sql = ColumnData.getSql_Update(u);
    }
    
    System.out.println("SAVE(): " + sql);
    
    MySqlQueryUtil.update(destinationData.databaseData, sql);
    // TODO - deal with truncation error ~~~~~~~~~~~~~~~~~~~~~~~~~~
  }
 
  private long doesDataExist() {
   String where = " WHERE " + ColumnData.getSql_IdentitiesWhere(columnData);
   String sql = "SELECT `" + destinationData.primaryKeyName + "` FROM `" + destinationData.table + "` " + where;
   long primaryKeyId = MySqlQueryUtil.queryLong(destinationData.databaseData, sql); 
   return primaryKeyId; 
  }
  
  /**
   * process each row > 0 against the flat file settings
   * 
   * @param row
   * @param values
   * @return
   */
  private String[] preProcessFlatFileValues(long row, String[] values) {
    for (int i=0; i < values.length; i++) {
      values[i] = evaluate(row, i, values[i]);
    }
    return values;
  }

  /**
   * create destination table
   */
  private void createTable() {
    MySqlTransformUtil.createTable(destinationData.databaseData, destinationData.table, destinationData.primaryKeyName);
  }
  
  
  
  
  /**
   * TODO change the way I add values
   *  
   * @param values
   * @return
   */
  private String[] extendColumns(String[] values) {
    
    int extendCount = 0;
    
    if (destinationData != null && destinationData.setSrcFileIntoColumn == true) {
     extendCount++; 
    }
    
    int newCount = extendCount + values.length;
    String[] c = new String[newCount];
    
    for (int i=0; i < values.length; i++) {
      c[i] = values[i];
    }
    
    int b = values.length;
    if (destinationData != null && destinationData.setSrcFileIntoColumn) {
      c[b] = "SrcFile";
      b++;
    }
    
    return c;
  }
  
  /**
   * TODO change the way i do this
   * 
   * @param row
   * @param values
   * @return
   */
  private String[] extendValues(int row, String[] values) {
    
    int extendCount = 0;
    
    if (destinationData != null && destinationData.setSrcFileIntoColumn == true) {
     extendCount++; 
    }
    
    int newCount = extendCount + values.length;
    String[] v = new String[newCount];
    
    for (int i=0; i < values.length; i++) {
      v[i] = values[i];
    }
    
    int b = values.length;
    if (destinationData != null && destinationData.setSrcFileIntoColumn) {
      v[b] = file.getAbsolutePath();
      b++;
    }
    
    return v;
  }
  
  
  private void listValues(int index, String[] values) {

    String s = "";
    for (int i=0; i < values.length; i++) {
      s += ", " + values[i];
    }

    System.out.println(index + ". " + s);
  }
  
  private ColumnData[] stopAtColumnsCount(ColumnData[] columns) {
    int c = columns.length;
    if (destinationData != null && destinationData.stopAtColumnCount > 1 && columns.length > destinationData.stopAtColumnCount) {
      c = destinationData.stopAtColumnCount;
    }
    ColumnData[] b = new ColumnData[c];
    for (int i=0; i < c; i++) {
      b[i] = columns[i];
    }
    return b;
  }
  
  private String[] stopAtColumnCount(String[] values) {
    
    int c = values.length;
    
    if (destinationData != null && destinationData.stopAtColumnCount > 1 && values.length > destinationData.stopAtColumnCount) {
      c = destinationData.stopAtColumnCount;
    }
    
    String[] b = new String[c];
    for (int i=0; i < c; i++) {
      b[i] = values[i];
    }
    return b;
  }
}
