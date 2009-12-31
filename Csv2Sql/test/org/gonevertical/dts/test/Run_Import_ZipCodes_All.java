package org.gonevertical.dts.test;

import java.io.File;
import java.net.URISyntaxException;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.data.FieldData;
import org.gonevertical.dts.data.SourceData;
import org.gonevertical.dts.lib.sql.transformlib.MySqlTransformLib;
import org.gonevertical.dts.v2.DestinationData_v2;
import org.gonevertical.dts.v2.Indexing;
import org.gonevertical.dts.v2.Optimise_v2;
import org.gonevertical.dts.v2.ProcessImport;


public class Run_Import_ZipCodes_All {

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
    String table = "import_zipcodes_test_all";
    
    // destination settings
    destinationData = new DestinationData_v2();
    destinationData.displayElapsedTime();
    destinationData.setData(databaseData, changeColumns, identities, table);
    
    // Settings
    destinationData.dropTable = false;
    destinationData.optimise = true;
    destinationData.debug = 1;
    destinationData.optimise_RecordsToExamine = 300;
    
    ProcessImport p = new ProcessImport(sourceData, destinationData);
    p.runImport();
    destinationData.displayElapsedTime();
    
    

  }
  
}
