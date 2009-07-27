package com.tribling.csv2sql.v2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import com.csvreader.CsvReader;
import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DestinationData;
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

    // TODO - create identity index
    // TODO ~~~~~~~~~~~~~~~~~~~~~~~~
    
    // loop through data rows
    iterateRowsData(fileIndex);

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

    
    /* TODO
    // loop through data rows
    iterateRowsData(fileIndex);
    */
    
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
    ColumnData c1 = new ColumnData();
    c1.setColumnName("DateCreated");
    c1.setType("DATETIME DEFAULT NULL");
    MySqlTransformUtil.createColumn(destinationData.databaseData, c1);
    
    ColumnData c2 = new ColumnData();
    c2.setColumnName("DateCreated");
    c2.setType("DATETIME DEFAULT NULL");
    MySqlTransformUtil.createColumn(destinationData.databaseData, c2);
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
      System.out.println("getColumnsInHeader: couln't read columns");
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
     
      // pre-process flat file values
      header[i] = evaluate(0, i, header[i]);
      
      // change Column Name by using Field matching
      header[i] = changeColumnName(header[i]);
      
      if (destinationData != null && destinationData.firstRowHasNoFieldNames == true && i < header.length - 1) { 
        columnData[i].setColumnName(" "); // this will change into 'c0','c1',... column names
      } else {
        columnData[i].setColumnName(header[i]);
      }
      
      // is this column used for identity
      if (isThisColumnUsedForIdentity(columnData[i], header[i])) {
        columnData[i].setIdentity(true);
      }
      
    }
    return columnData;
  }
  
  /**
   * is this Column used for identity
   * 
   * @param columnData
   * @param headerValue
   * @return
   */
  private boolean isThisColumnUsedForIdentity(ColumnData columnData, String headerValue) {
    // find the destination field by comparing source field
    int index = FieldData.getSourceFieldIndex(destinationData.changeColumn, headerValue);
    
    // no matches found in field data
    if (index < 0) {
      return false;
    }
    
    // get the destination field
    String destinationField = destinationData.changeColumn[index].destinationField;
    
    boolean b = false;
    if (columnData.getColumnName().toLowerCase().equals(destinationField.toLowerCase()) ) {
      b = true;
    }
    return b;
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
    
    // get values from file
    try {
      values = reader.getValues();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    // pre-process values in row
    values = preProcessFlatFileValues(index + 1, values);
    
    // add values to columnData
    columnData = ColumnData.addValues(columnData, values);
    
    
    // TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // TODO - test columns length fits into db
    // TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
    // save Columns/Values to db
    save();
  }

  /**
   * save the column's values into db
   */
  private void save() {
    
    long primaryKeyId = doesDataExist();
    
    String sql = "";
    if (primaryKeyId > 0) {
      sql = ColumnData.getSql_Insert(columnData);
    } else { 
      sql = ColumnData.getSql_Update(columnData, primaryKeyId);
    }
    
    System.out.println("SAVE(): " + sql);
    
    //MySqlQueryUtil.update(destinationData.databaseData, sql);
    // TODO - deal with truncation error ~~~~~~~~~~~~~~~~~~~~~~~~~~
  }
  
  private long doesDataExist() {
   String where = " WHERE " + ColumnData.getSql_IdentitiesWhere(columnData);
   String primaryKeyName = ColumnData.getPrimaryKey_Name(columnData);
   String sql = "SELECT `" + primaryKeyName + "` FROM `" + destinationData.table + "` " + where;
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
