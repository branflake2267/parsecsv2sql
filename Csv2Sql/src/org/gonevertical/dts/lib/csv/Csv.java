package org.gonevertical.dts.lib.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;

public class Csv {

  public Csv() {
  }
  
  public CsvReader open(File file, char delimiter) {
    CsvReader csvRead = null;
    try {
      if (Character.toString(delimiter) == null) {
        System.out.println("openFileAndRead: You forgot to set a delimiter. Exiting.");
        System.exit(1);
      }
    } catch (Exception e1) {
      System.out.println("openFileAndRead: You forgot to set a delimiter. Exiting.");
      e1.printStackTrace();
      System.exit(1);
    }
    try {     
      csvRead = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("CSV Reader, Could not open CSV Reader");
      e.printStackTrace();
    }
    return csvRead;
  }
  
  public String[] getColumns(CsvReader csvRead) {
    String[] columns = null;
    try {
      csvRead.readHeaders();
      columns = csvRead.getHeaders();
    } catch (IOException e) {
      System.out.println("Csv.getColumnsFromCsv(): couln't read header");
      e.printStackTrace();
    }
    return columns;
  }
  
}
