package org.gonevertical.dts.openoffice;

import java.io.File;

public class SpreadSheetData {
  
  private File file = null;
  
  private char delimiter;
  
  private String sheetName = null;
  
  public SpreadSheetData(String sheetName, File file, char delimiter) {
    this.file = file;
    this.sheetName = sheetName;
    this.delimiter = delimiter;
  }

  public File getFile() {
    return file;
  }
  
  public String getSheetName() {
    return sheetName;
  }
  
  public char getDelimiter() {
    return delimiter;
  }
  
}
