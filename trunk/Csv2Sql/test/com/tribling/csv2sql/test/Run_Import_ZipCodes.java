package com.tribling.csv2sql.test;

import java.io.File;
import java.net.URISyntaxException;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.FieldData;
import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformAlterUtil;
import com.tribling.csv2sql.v2.DestinationData_v2;
import com.tribling.csv2sql.v2.Indexing;
import com.tribling.csv2sql.v2.Optimise_v2;
import com.tribling.csv2sql.v2.ProcessImport;

public class Run_Import_ZipCodes {

  // source data point
  private static SourceData sourceData = null;
  
  // destination data point
  private static DestinationData_v2 destinationData = null;
  
  
  public static void main(String[] args) {
    run();
  }
  
  public static void run() {
    
    // set path dynamically
    File executionlocation = null;
    try {
      executionlocation = new File(Run_Test_Import_v1.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String execPath = executionlocation.getParent();
    String pathToFile = execPath + "/data/export/zipcodes_0.csv"; 
    
    // source
    sourceData = new SourceData();
    sourceData.delimiter = ",".charAt(0);
    sourceData.file = new File(pathToFile);
    
    // des identity fields
    FieldData[] identities = new FieldData[1];
    identities[0] = new FieldData();
    identities[0].sourceField = "zip_code";
    identities[0].destinationField = "zip_code";
    
    // des change columns
    FieldData[] changeColumns = null;

    // database settings
    DatabaseData databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    String table = "import_zipcodes_test";
    
    // destination settings
    destinationData = new DestinationData_v2();
    destinationData.displayElapsedTime();
    destinationData.setData(databaseData, changeColumns, identities, table);
    
    // Settings
    destinationData.dropTable = true;
    destinationData.optimise = false;
    destinationData.stopAtRow = 1000; // lets only import a 100 rows for testing
    
    destinationData.debug = 0;
    
    ProcessImport p = new ProcessImport(sourceData, destinationData);
    p.runImport();
    destinationData.displayElapsedTime();
    
    
    
    ColumnData[] columnData = MySqlTransformAlterUtil.queryColumns(destinationData.databaseData, table, "`FIELD`='latitude'");
    Optimise_v2 o = new Optimise_v2(destinationData);
    o.run(columnData);
    
    destinationData.displayElapsedTime();
    
    destinationData.dropTable = false;
    pathToFile = execPath + "/data/export/zipcodes_0_testalter1.csv"; 
    sourceData.file = new File(pathToFile);
    ProcessImport p2 = new ProcessImport(sourceData, destinationData);
    p2.runImport();
    
    // optimise
    o.run(columnData);
    
    destinationData.displayElapsedTime();
    
    destinationData.dropTable = false;
    pathToFile = execPath + "/data/export/zipcodes_0_testalter2.csv"; 
    sourceData.file = new File(pathToFile);
    ProcessImport p3 = new ProcessImport(sourceData, destinationData);
    p3.runImport();
    
    // optomise
    o.run(columnData);
    
    destinationData.displayElapsedTime();
    
    // index
    ColumnData[] indexColumns = new ColumnData[3];
    indexColumns[0] = new ColumnData();
    indexColumns[1] = new ColumnData();
    indexColumns[2] = new ColumnData();
    
    indexColumns[0] = MySqlTransformAlterUtil.queryColumn(destinationData.databaseData, table, "zip_code");
    indexColumns[1] = MySqlTransformAlterUtil.queryColumn(destinationData.databaseData, table, "state_abbreviation");
    indexColumns[2] = MySqlTransformAlterUtil.queryColumn(destinationData.databaseData, table, "latitude");
    
    Indexing index = new Indexing(destinationData);
    index.runIndexColumns(indexColumns);
    
    destinationData.displayElapsedTime();
    
    // optimise all
    o.run();
    
    destinationData.displayElapsedTime();
  }
  
}
