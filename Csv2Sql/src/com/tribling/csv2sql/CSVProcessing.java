package com.tribling.csv2sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;
import com.sun.org.apache.bcel.internal.generic.DDIV;
import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;

public class CSVProcessing extends FlatFileProcessing {

  // work on this file
  private File file = null;

  // this file has this delimter
  private char delimiter;

  // csv reader 2.0
  private CsvReader reader = null;

  // sql methods
  private Optimise sql = new Optimise();

  // destination data settings, used in first column processing
  private DestinationData destinationData = null;

  /**
   * constructor
   */
  public CSVProcessing() {
  }
  
  /**
   * set the settings
   * 
   * @param delimiter
   * @param destinationData
   * @param matchFields
   */
  protected void setData(char delimiter, DestinationData destinationData, MatchFieldData[] matchFields) {
    this.delimiter = delimiter;
    this.destinationData  = destinationData;

    try {
      sql.setDestinationData(destinationData);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("ERROR: CSVProcessing: forogot destinationData is null");
      System.exit(1);
    }
    sql.setMatchFields(matchFields);

  }

  /**
   * when reading a directory of files, don't drop table when moving to the second file notification
   */
  public void dropTableOff() {
    sql.dropTableOff();
  }

  /**
   * parse the file, insert/update sql, then optimise it if needed.
   *  
   * @param fileIndex - when reading a director of files(.list) this is the index number of which file its on.
   * @param file - the file to parse
   */
  protected void parseFile(int fileIndex, File file) {
    this.file = file;

    // open a sql connection
    sql.openConnection();

    // create table
    sql.createTable();

    // create columns importid, datecreated, dateupdated used for tracking
    sql.createImportTrackingColumns();

    // open the file
    openFileAndRead();

    // create columns from file
    getColumnsInHeader();

    // loop through data rows
    iterateRowsData(fileIndex);

    // optimise table (if set on)
    sql.runOptimise();

    // delete empty columns (if set on)
    sql.deleteEmptyColumns();

    // done with connection
    sql.closeConnection();
  }

  /**
   * open file and start reading it
   */
  private void openFileAndRead() {
    try {
      reader = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("CSV Reader, Could not open CSV Reader");
      e.printStackTrace();
    }
  }

  /**
   * get columns from file, and create them in database if need be
   *  
   *  ~~~ 1st row processing ~~~~
   * 
   * @throws Exception
   */
  private void getColumnsInHeader() {
    
    ColumnData[] columns = null;	
    try {
      reader.readHeaders();
      columns = makeColumnlist(reader.getHeaders());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // insert columns into database if need be
    if (columns != null) {
      sql.createColumns(columns);

    } else {
      System.err.println("CSV Reader could not get columns");
      System.exit(1);
    }

  }

  /**
   * iterate through data in file
   * 
   * @param indexFile
   */
  private void iterateRowsData(int indexFile) {

    String[] values = null;
    int index = 0;
    
    // when the first row is data, need to move up one row to start
    if (destinationData.firstRowHasNoFieldNames == true) {
      index--;
    }
    
    try {
      while (reader.readRecord()) {

        values = reader.getValues();

        values = processRow(index+1, values);
        
        // add data to table
        sql.addData(indexFile, index, values);

        // debug
        //listValues(index, values);

        index++;
      }
    } catch (IOException e) {
      System.out.println("Error: Can't loop through data!");
      e.printStackTrace();
    }

  }

  /**
   * make a column 1(header) list of the fields, 
   *   which will always be needed when inserting/updating the data into sql
   * 
   *   ~~~ first row processsing ~~~
   * 
   * @param columns
   * @return
   */
  private ColumnData[] makeColumnlist(String[] columns) {

    ColumnData[] cols = new ColumnData[columns.length];
    for(int i=0; i < columns.length; i++) {

      // FlatFileSettings processing - pre-process the values before sql
      columns[i] = evaluate(0, i, columns[i]);
      
      cols[i] = new ColumnData();
      
      if (destinationData.firstRowHasNoFieldNames == true) {
        cols[i].column = " "; // this will change into 'c0','c1',... column names
      } else {
        cols[i].column = columns[i];
      }
      
    }

    return cols;
  }

  private void listValues(int index, String[] values) {

    String s = "";
    for (int i=0; i < values.length; i++) {
      s += ", " + values[i];
    }

    System.out.println(index + ". " + s);
  }

  /**
   * process each row > 0 against the flat file settings
   * 
   * @param row
   * @param values
   * @return
   */
  private String[] processRow(int row, String[] values) {
    for (int i=0; i < values.length; i++) {
      values[i] = evaluate(row, i, values[i]);
    }
    return values;
  }

  
}
