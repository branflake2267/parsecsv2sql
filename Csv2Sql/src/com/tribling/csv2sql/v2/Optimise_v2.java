package com.tribling.csv2sql.v2;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.SourceData;
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
  
  private ColumnData[] columnData = null;

  // resize these columns
  private ArrayList<ColumnData> resize = new ArrayList<ColumnData>();
  
  public Optimise_v2(DestinationData_v2 destinationData) {
    this.destinationData = destinationData;
  }
  
  public void run() {
    process();
  }

  private void process() {
 
    // get columns
    String where = null; // all columns
    columnData = MySqlTransformUtil.queryColumns(destinationData.databaseData, destinationData.table, where);
    
    analyzeColumns();
    
 
  }
  
  /**
   * analyze columns to go smaller
   */
  private void analyzeColumns() {
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
    
    if (columnData.getType().toLowerCase().contains("text")) {
      int currentSize = columnData.getCharLength();
      
      String sql = ColumnData.getSql_GetMaxCharLength(destinationData.databaseData, columnData);
      int resize = MySqlQueryUtil.queryInteger(destinationData.databaseData, sql);
      
      if (currentSize == resize) {
        return;
      }
      
      columnData.setCharLength(resize);
      MySqlTransformUtil.alterColumn(destinationData.databaseData, columnData);
    }
    
  }
  
  
}
