package com.tribling.csv2sql.lib.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;

public class MoveFileData {

  public static final int MATCH_HEADERS = 1;
  public static final int MATCH_FILENAME = 2;
  private int matchHow = 0;
  
  // TODO change to private
  public String matchHeaderValues = null;
  
  // TODO change to private
  public char delimiter;
  
  // TODO change to private
  public String pathToMoveToDir = null;
  
  // regex a file name by this
  private String matchFileNameRegex = null;
  
  /**
   * set a sample file to get the matchheaderValues from
   * 
   * @param fileOrdir
   */
  public void setMatchHeaderValues(File fileOrdir) {
    matchHow = MATCH_HEADERS;
    
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
  
  public void setMatchByFileName(String regex) {
    this.matchHow = MATCH_FILENAME;
    this.matchFileNameRegex = regex;
  }
  
  public int getMatchHow() {
    return this.matchHow;
  }
  
  public String getHeaders() {
    return this.matchHeaderValues;
  }
  
  public char getDelimiter() {
    return this.delimiter;
  }
  
  public String getMovePath() {
    return this.pathToMoveToDir;
  }
  
  public String getFileNameRegex() {
    return this.matchFileNameRegex;
  }


  
}
