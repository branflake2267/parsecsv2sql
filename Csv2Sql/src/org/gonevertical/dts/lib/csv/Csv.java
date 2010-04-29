package org.gonevertical.dts.lib.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;

public class Csv {

	private Logger logger = Logger.getLogger(Csv.class);
	
  public Csv() {
  }
  
  public CsvReader open(File file, char delimiter) {
    CsvReader csvRead = null;
    try {
      if (Character.toString(delimiter) == null) {
        logger.fatal("Csv.open(): openFileAndRead: You forgot to set a delimiter. Exiting.");
        System.exit(1);
      }
    } catch (Exception e1) {
    	logger.fatal("Csv.open(): You forgot to set a delimiter. Exiting.", e1);
      e1.printStackTrace();
      System.exit(1);
    }
    try {     
      csvRead = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      logger.error("Csv.CsvReader(): Could not open CSV Reader", e);
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
      logger.error("Csv.getColumns(): couln't read headers: ", e);
      e.printStackTrace();
    }
    return columns;
  }
  
}
