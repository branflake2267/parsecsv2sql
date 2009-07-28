package com.tribling.csv2sql.test;

import java.io.File;
import java.net.URISyntaxException;

import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.FieldData;
import com.tribling.csv2sql.data.FlatFileSettingsData;
import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.v1.FileProcessing;
import com.tribling.csv2sql.v1.Optimise;
import com.tribling.csv2sql.v1.SQLProcessing;

public class Run_Test_Import_v1 {

  private static SourceData sourceData = null;
  
  private static FieldData[] matchFields = null;
  
  private static DestinationData dd = null;
  
  private static FlatFileSettingsData ffsd = null;
  
  public static void main(String[] args) {
    run();
  }
  
  public static void run() {
    
    setParameters();
    
    importData();

    optimiseTable();
    
    indexTable();
    
    customSql();
    
    // TODO -- add custom query a duplicate in to test this
    deleteDuplicates();
  }
  
  private static void setParameters() {
    char delimiter = ',';

    File executionlocation = null;
    try {
      executionlocation = new File(Run_Test_Import_v1.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String path = executionlocation.getPath();
    String file = path + "/com/tribling/csv2sql/test/test.txt"; 
    
    sourceData = new SourceData();
    sourceData.delimiter = delimiter;
    sourceData.file = new File(file);
    
    dd = new DestinationData();
    dd.dropTable = false;
    dd.checkForExistingRecordsAndUpdate = true;
    dd.databaseType = "MySql";
    dd.host = "192.168.10.79";
    dd.database = "test";
    dd.username = "test";
    dd.password = "test";
    dd.port = "3306";
    dd.tableSchema = "";
    dd.table = "import_test"; 
    
    // field mapping
    // when originating flat file fields match source var, then destinationfield var is put in
    matchFields = new FieldData[2];
    matchFields[0] = new FieldData();
    matchFields[1] = new FieldData();
    
    matchFields[0].sourceField = "a";
    matchFields[0].destinationField = "aaaa";
   
    matchFields[1].sourceField = "d";
    matchFields[1].destinationField = "dododo";
    
    // set records uniqueness 
    FieldData[] idents = new FieldData[2];
    idents[0] = new FieldData();
    idents[1] = new FieldData();
    
    idents[0].sourceField = "a";
    idents[0].destinationField = "aaaa";
    
    idents[1].sourceField = "b";
    idents[1].destinationField = "b";
    
    dd.identityColumns = idents;
    
    // transform on the fly - transform before sql processing
    ffsd = new FlatFileSettingsData();
    // transforms the column e to dateString
    ffsd.setchangeValueIntoDateStringFormat("e"); 
  }
  
  /**
   * import the flat file into sql
   */
  private static void importData() {
    FileProcessing p = new FileProcessing();
    p.setData(ffsd);
    p.setData(sourceData, dd, matchFields);
  }

  /**
   * optimize the sql table
   */
  private static void optimiseTable() {
    
    dd.deleteEmptyColumns = true;
    dd.optimise = true;
    dd.optimise_TextOnly = true; // only optimise text columns
    
    // optimise the data structure
    Optimise o = new Optimise();
    try {
      o.setDestinationData(dd);
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    // transform column dt to a new column datetime
    // dt column will get copied and transformed to datetime
    o.setColumnToTransformTo("datetime"); 
    o.formatColumn_Date_DateTime("dt");
    
    // optimise the entire table
    o.runOptimise();
  }
  
  private static void indexTable() {

    // index
    String[] c1 = new String[3];
    c1[0] = "d";
    c1[1] = "3";
    c1[2] = "j_num";

    // full index
    String[] c2 = new String[1];
    c2[0] = "l";

    Optimise o = new Optimise();
    try {
      o.setDestinationData(dd);
    } catch (Exception e) {
      e.printStackTrace();
    }
   o.runIndexing(c1);
   o.runIndexing_FullText(c2);
   o.closeConnection();
  }
  
  private static void customSql() {
    
    String sql = "";
    if (dd.getDbType() == 1) {
      sql = "DELETE FROM " + dd.table + " WHERE (dt= '') OR (dt IS NULL)";
    } else if (dd.getDbType() == 2){
      sql = "DELETE FROM " + dd.table + " WHERE (dt= '') OR (dt IS NULL)";
    }

    SQLProcessing p = new SQLProcessing();
    try {
      p.setDestinationData(dd);
    } catch (Exception e) {
      e.printStackTrace();
    }
    p.openConnection();
    p.updateSql(sql);
    p.closeConnection();
  }
  
  private static void deleteDuplicates() {
    
    Optimise o = new Optimise();
    try {
      o.setDestinationData(dd);
    } catch (Exception e) {
      e.printStackTrace();
    }
    o.deleteDuplicates();
  }

}
