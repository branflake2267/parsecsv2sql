package com.tribling.csv2sql.test;

import java.io.File;
import java.net.URISyntaxException;

import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.FieldData;
import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.v2.DestinationData_v2;
import com.tribling.csv2sql.v2.Process;

public class Run_Import_Tests {


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
    
    
    // source
    sourceData = new SourceData();
    
    // des identity fields
    FieldData[] identities = null;
    
    // des change columns
    FieldData[] changeColumns = null;
    
    // database settings
    DatabaseData databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    String table = "import_states_test";
    // destination settings
    destinationData = new DestinationData_v2();
    destinationData.setData(databaseData, changeColumns, identities, table);
    
    destinationData.optimise = true;
    
    destinationData.stopAtRow = 100;
    
    FieldData[] i1 = new FieldData[3];
    i1[0] = new FieldData();
    i1[1] = new FieldData();
    i1[2] = new FieldData();
    i1[0].sourceField = "series_id";
    i1[0].destinationField = "series_id";
    i1[1].sourceField = "period";
    i1[1].destinationField = "period";
    i1[2].sourceField = "value";
    i1[2].destinationField = "value";
    
    String pathToFile = execPath + "/data/export/test_1.tab"; 
    sourceData.file = new File(pathToFile);
    sourceData.delimiter = "\t".charAt(0);
    destinationData.table = "test1";
    destinationData.identityColumns = i1;
    
    // insert
    destinationData.dropTable = true;
    Process p = new Process(sourceData, destinationData);
    p.runImport();
    
    
    
    // repeat - update
    destinationData.dropTable = false;
    Process p2 = new Process(sourceData, destinationData);
    p2.runImport();
    
    
    
    FieldData[] i2 = new FieldData[1];
    i2[0] = new FieldData();
    i2[0].sourceField = "series_id";
    i2[0].destinationField = "series_id";

    pathToFile = execPath + "/data/export/test_2.tab"; 
    sourceData.file = new File(pathToFile);
    sourceData.delimiter = "\t".charAt(0);
    destinationData.table = "test2";
    destinationData.identityColumns = i2;
    
    destinationData.dropTable = true;
    Process p3 = new Process(sourceData, destinationData);
    p3.runImport();
    
    destinationData.dropTable = false;
    Process p4 = new Process(sourceData, destinationData);
    p4.runImport();

    
  }
  
}
