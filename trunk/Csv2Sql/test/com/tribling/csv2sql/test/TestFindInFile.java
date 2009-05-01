package com.tribling.csv2sql.test;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.data.FlatFileSettingsData;
import com.tribling.csv2sql.data.SourceData;

public class TestFindInFile {

  public TestFindInFile() {
    
  }

  public void run() {
    
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    int row = -1; 
    int column = 0;
    ffsd.findInFile_byRegex(row, column, "11201");
    
    char delimiter = ',';
    
    String system = System.getProperty("os.name").toLowerCase();
    String file = "";
    if (system.contains("win")) {
      file = "";
    } else {
      file = "/home/branflake2267/data/test.csv";
    }

    SourceData sourceData = new SourceData();
    sourceData.delimiter = delimiter;
    sourceData.file = new File(file);
    
    
    FileProcessing process = new FileProcessing();
    process.setData(ffsd);
    process.setData(sourceData);
    File foundFile = process.findMatchInFile();
    System.out.println("found the file: " + foundFile.getName());
  }
  
}
