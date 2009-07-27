package com.tribling.csv2sql.test;

import java.io.File;
import java.net.URISyntaxException;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.Optimise;
import com.tribling.csv2sql.SQLProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.FieldData;
import com.tribling.csv2sql.data.FlatFileSettingsData;
import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.v2.DestinationData_v2;
import com.tribling.csv2sql.v2.FileProcessing_v2;
import com.tribling.csv2sql.v2.Optimise_v2;
import com.tribling.csv2sql.v2.Process;

public class Run_Import_States {

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
    String path = executionlocation.getPath();
    String file = path + "/data/export/states_0.csv"; 
    
    // TODO set parameters
    
    Process p = new Process(sourceData, destinationData);
    p.runImport();
    


  }
  



  

}
