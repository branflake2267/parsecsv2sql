package org.gonevertical.dts.v2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.FieldData;
import org.gonevertical.dts.data.FieldDataComparator;
import org.gonevertical.dts.data.SourceData;
import org.gonevertical.dts.lib.csv.Csv;
import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnmulti.ColumnLibFactory;
import org.gonevertical.dts.lib.sql.querylib.QueryLib;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;

import com.csvreader.CsvReader;

public class CsvProcessing_v2 extends FlatFileProcessing_v2 {

  // supporting libraries
  private QueryLib ql = null;
  private TransformLib tl = null;
  private ColumnLib cl = null;
  
  private Csv csv = null;
  
  // csv reader 2.0
  private CsvReader csvRead = null;

  // work on this file
  private File file = null;
  
  // data source
  private SourceData sd = null;
  
  // data destination
  private DestinationData_v2 dd = null;

  // columns being imported 
  private ColumnData[] columnData = null;
  
  private ColumnData[] defaultColumns = null;
  
  private int rowIndex = 0;
  
  /**
   * constructor
   */
  public CsvProcessing_v2(SourceData sourceData, DestinationData_v2 destinationData) {
    this.sd = sourceData;
    this.dd = destinationData;
    
    // preprocess these settings
    if (destinationData.ffsd != null) {
      setData(destinationData.ffsd);  
    }
    
    // setup injector libraries
    setSupportingLibraries();
  }
  
  /**
   * guice injects the libraries needed for the database
   */
  private void setSupportingLibraries() {
    ql = QueryLibFactory.getLib(dd.databaseData.getDatabaseType());
    cl = ColumnLibFactory.getLib(dd.databaseData.getDatabaseType());
    tl = TransformLibFactory.getLib(dd.databaseData.getDatabaseType());
    csv = new Csv();
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
    setupColumns();

    //markColumnsThatAreIdents();
     
    createIdentitiesIndex();
    
    // loop through data rows
    iterateRowsData(fileIndex);

  }
  
  private void createIdentitiesIndex() {
    if (dd.identityColumns == null) {
      return;
    }
   
    String sql = cl.getSql_IdentitiesIndex(dd.databaseData, columnData);
    if (sql == null) {
      //System.out.println("ERROR: createIdentitiesIndex(): Fix the identities.");
      //System.exit(1);
      System.out.println("skipping identities indexing, probably already created. createIdentitiesIndex()");
      return;
    }
    ql.update(dd.databaseData, sql);
  }

  /**
   * look for match only
   *   
   *   TODO - finish method - this needs to find a match
   *   
   * @param fileIndex
   * @param file
   * @return
   */
  protected boolean parseFile_Match(int fileIndex, File file) {
    this.file = file;

    // open the csv file for reading
    openFileAndRead();

    // compare version 1 
    
    return false;
  }

  /**
   * open file and start reading it
   */
  private void openFileAndRead() {
    csvRead = csv.open(file, sd.delimiter);
  }
  
  /**
   * setup columns from csv header
   */
  private void setupColumns() {
    String[] columns = csv.getColumns(csvRead);
    columnData = processColumnsToColumnData(columns);
    tl.createColumn(dd.databaseData, columnData);
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
  private ColumnData[] processColumnsToColumnData(String[] header) {
    ColumnData[] columnData = new ColumnData[header.length];
    for(int i=0; i < header.length; i++) {
      columnData[i] = new ColumnData();
      columnData[i].setTable(dd.table);
     
      // is this column used for identity
      if (isThisColumnUsedForIdentity(columnData[i], header[i])) {
        columnData[i].setIdentity(true);
      }
      
      // pre-process flat file values
      header[i] = evaluate(0, i, header[i]);
      
      // change Column Name by using Field matching
      header[i] = changeColumnName(header[i]);
      
      if (dd != null && 
          dd.firstRowHasNoFieldNames == true && 
          i < header.length - 1) { 
        columnData[i].setColumnName(" "); // this will change into 'c0','c1',... column names
      } else {
        columnData[i].setColumnName(header[i]);
      }
      
    }
    return columnData;
  }
  
  /**
   * create default columns
   */
  private void createDefaultFields() {
    defaultColumns = new ColumnData[2];
    defaultColumns[0] = new ColumnData();
    defaultColumns[1] = new ColumnData();
    
    defaultColumns[0].setTable(dd.table);
    defaultColumns[0].setColumnName(dd.dateCreated);
    defaultColumns[0].setType("DATETIME DEFAULT NULL");
    defaultColumns[0].setValueAsFunction("NOW()");
   
    defaultColumns[1].setTable(dd.table);
    defaultColumns[1].setColumnName(dd.dateUpdated);
    defaultColumns[1].setType("DATETIME DEFAULT NULL");
    defaultColumns[1].setValueAsFunction("NOW()");
    
    tl.createColumn(dd.databaseData, defaultColumns);
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
    int index = FieldData.getSourceFieldIndex(dd.identityColumns, headerValue);
    
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
    if (dd.changeColumn == null) {
      return columnName;
    }
    Comparator<FieldData> searchByComparator = new FieldDataComparator();
    Arrays.sort(dd.changeColumn);
    FieldData searchFor = new FieldData();
    searchFor.sourceField = columnName;
    int index = Arrays.binarySearch(dd.changeColumn, searchFor, searchByComparator);
    if (index > -1) {
      columnName = dd.changeColumn[index].destinationField;
    }
    return columnName;
  }
  
  /**
   * iterate through data in file
   * 
   * @param indexFile
   */
  private void iterateRowsData(int indexFile) {
    rowIndex = 0;
    
    // when the first row is data, need to move up one row to start
    if (dd != null && dd.firstRowHasNoFieldNames == true) {
      rowIndex--;
    }
    
    try {
      while (csvRead.readRecord()) {
        process(rowIndex, csvRead);
        
        // stop early
        if (dd != null && dd.stopAtRow == rowIndex) {
          return;
        }
        rowIndex++;
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
    columnData = cl.addValues(columnData, values);
    
    testColumnValueTypes();
    testColumnValueSizes();
    
    // save Columns/Values to db
    save();
  }
  
  /**
   * test the value type will fit into the sql type
   */
  private void testColumnValueTypes() {
    
    for (int i=0; i < columnData.length; i++) {
      String vraw = columnData[i].getValue();
      boolean testTypeThrown = columnData[i].getTestTypeThrow();
      columnData[i].setValue(vraw);
      
      // alter back to text
      if (testTypeThrown == true) {
        columnData[i].setType("TEXT DEFAULT NULL");
        tl.alterColumn(dd.databaseData, columnData[i]);
      }
   
    }
    
  }

  /**
   * does the values lengths fit into sql column, so not to truncate
   */
  private void testColumnValueSizes() {
    for (int i=0; i < columnData.length; i++) {
      columnData[i].alterColumnSizeBiggerIfNeedBe(dd.databaseData);
    }
  }

  /**
   * save the column's values into db
   */
  private void save() {
    
    long primaryKeyId = doesDataExist();
     
    String sql = "";
    if (primaryKeyId > 0) {
      ColumnData primaryKeyColumn = new ColumnData();
      primaryKeyColumn.setTable(dd.table);
      primaryKeyColumn.setIsPrimaryKey(true);
      primaryKeyColumn.setColumnName(dd.primaryKeyName);
      primaryKeyColumn.setValue(primaryKeyId);
      
      boolean skip = compareBefore(columnData, primaryKeyColumn);
      if (skip == true) {
        dd.debug("skipping b/c of compare.!!!!!!!!!!!!!!!!!!");
        return;
      }
      ColumnData[] u = cl.merge(columnData, defaultColumns[1]); // add update column
      u = cl.merge(u, primaryKeyColumn);
      sql = cl.getSql_Update(u);
    } else { 
      ColumnData[] i = cl.merge(columnData, defaultColumns[0]); // add insert column
      sql = cl.getSql_Insert(i);
    }
    
    dd.debug(rowIndex + ". SAVE(): " + sql);
    
    ql.update(dd.databaseData, sql);
    // TODO - deal with truncation error ~~~~~~~~~~~~~~~~~~~~~~~~~~
    // TODO - truncation should rarely happen here, b/c this is tested for earlier.
    // TODO - set loggin
  }
 
  /**
   * run a comparison on the columndata before saving it
   * 
   * @param columnData
   * @param primKeyColumn
   * @return
   */
  private boolean compareBefore(ColumnData[] columnData, ColumnData primKeyColumn) {
    
    if (dd.compareBeforeUpdate == null) {
      return false;
    }
    boolean b = false;
    
    String where = " WHERE `" + primKeyColumn.getColumnName() + "`='" + primKeyColumn.getValue() + "'";
    
    FieldData[] c = dd.compareBeforeUpdate;
    for (int i=0; i < c.length; i++) {
      ColumnData forColumnData = new ColumnData(primKeyColumn.getTable(), c[i].destinationField, "TEXT");
      int index = cl.searchColumnByName_NonComp(columnData, forColumnData);
      if (index > -1) {
        String sql = "SELECT " + c[i].destinationField + " FROM " + primKeyColumn.getTable() + " " + where;
        String beforeValue = columnData[index].getValue();
        String inTableValue = ql.queryString(dd.databaseData, sql);
        
        if (Integer.parseInt(beforeValue) < Integer.parseInt(inTableValue)) {
          b = true;
        }
        dd.debug("CsvProcessing_v2.compareBefore(): forTable:" + dd.table + ": before: " + beforeValue + " < inTable: " + inTableValue + " result: " + b);
      }
    }
    
    return b;
  }
  
  private long doesDataExist() {
   if (dd.identityColumns == null) {
     return -1;
   }
   String idents = cl.getSql_IdentitiesWhere(columnData);
   if (idents == null || idents.trim().length() == 0) {
     System.out.println("ERROR: doesDataExist(): Can't figure out the identies. exiting. (check delimiter?)");
     // TODO - what if? Should we continue???? 
   }
   String where = " WHERE " + idents;
   String sql = "SELECT `" + dd.primaryKeyName + "` FROM `" + dd.table + "` " + where;
   long primaryKeyId = ql.queryLong(dd.databaseData, sql); 
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
    tl.createTable(dd.databaseData, dd.table, dd.primaryKeyName);
  }
  
  
  
  
  /**
   * TODO change the way I add values
   *  
   * @param values
   * @return
   */
  private String[] extendColumns(String[] values) {
    
    int extendCount = 0;
    
    if (dd != null && dd.setSrcFileIntoColumn == true) {
     extendCount++; 
    }
    
    int newCount = extendCount + values.length;
    String[] c = new String[newCount];
    
    for (int i=0; i < values.length; i++) {
      c[i] = values[i];
    }
    
    int b = values.length;
    if (dd != null && dd.setSrcFileIntoColumn) {
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
    
    if (dd != null && dd.setSrcFileIntoColumn == true) {
     extendCount++; 
    }
    
    int newCount = extendCount + values.length;
    String[] v = new String[newCount];
    
    for (int i=0; i < values.length; i++) {
      v[i] = values[i];
    }
    
    int b = values.length;
    if (dd != null && dd.setSrcFileIntoColumn) {
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
    if (dd != null && dd.stopAtColumnCount > 1 && columns.length > dd.stopAtColumnCount) {
      c = dd.stopAtColumnCount;
    }
    ColumnData[] b = new ColumnData[c];
    for (int i=0; i < c; i++) {
      b[i] = columns[i];
    }
    return b;
  }
  
  private String[] stopAtColumnCount(String[] values) {
    
    int c = values.length;
    
    if (dd != null && dd.stopAtColumnCount > 1 && values.length > dd.stopAtColumnCount) {
      c = dd.stopAtColumnCount;
    }
    
    String[] b = new String[c];
    for (int i=0; i < c; i++) {
      b[i] = values[i];
    }
    return b;
  }
  
  
  private void markColumnsThatAreIdents() {
    if (dd.identityColumns == null) {
      return;
    }
    for (int i=0; i < columnData.length; i++) {
      for (int b=0; b < dd.identityColumns.length; b++) {
        if (columnData[i].getColumnName().toLowerCase().equals(dd.identityColumns[b].destinationField) == true) {
          columnData[i].setIdentity(true);
        }
      }
    }
  }
  
}
