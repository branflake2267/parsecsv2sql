package com.tribling.csv2sql.lib.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;

public class MoveFileData {

  public String matchHeaderValues = null;
  
  public char delimiter;
  
  public String pathToMoveToDir = null;
  
  /**
   * set a sample file to get the matchheaderValues from
   * 
   * TODO - what to do with non csv files
   * 
   * @param fileOrdir
   */
  public void setMatchHeaderValues(File fileOrdir) {
   
    File file = null;
    if (fileOrdir.isDirectory()) {
      file = fileOrdir.listFiles()[0];
    } else {
      file = fileOrdir;
    }
    
    CsvReader reader = null;
    try {     
      reader = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("doesFileHeaderMatchStr: Could not open CSV Reader");
      e.printStackTrace();
      return;
    }
    
    if (reader == null) {
      matchHeaderValues = null;
    }
    
    String[] header = null;
    try {
      reader.readHeaders();
      header = reader.getHeaders();
    } catch (IOException e) {
      System.out.println("doesFileHeaderMatchStr: could not read headers");
      e.printStackTrace();
      
    }
    
    if (header == null) {
      matchHeaderValues = null;
    }
    
    String sheader = "";
    for (int i=0; i < header.length; i++) {
      sheader += header[i];
    }
    
    matchHeaderValues = sheader;
  }
  
}
