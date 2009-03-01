package com.tribling.csv2sql;

import java.util.ArrayList;

import com.tribling.csv2sql.data.FlatFileSettingsData;

/**
 * process the flat file before processing
 * 
 * TODO Options, only work on a row, only work on a column, only work on a row and column...
 *
 * 
 * @author branflake2267
 *
 */
public class FlatFileProcessing {

  private ArrayList<FlatFileSettingsData> settings = new ArrayList<FlatFileSettingsData>();
  
  /**
   * constructor
   */
  public FlatFileProcessing() {
  }
  
  public void setchangeValue(int row, int column, String value) {
    
  }
  
  public void setchangeValueWhenEmptyMatch(int row, int column, String value) {
    
  }
  
  public void setchangeValueWithRegexMatch(int row, int column, String regex, String value) {
    
  }
  
  public void setchangeValueWithRegexReplace(int row, int column, String regex) {
    
  }
  
  /**
   * auto format a to best values, like date and phone numbers to consistent format 
   * @param row
   * @param column
   */
  public void setautoFormatValue(int row, int column) {
    
  }
  
  public void setsampleColumnType(int row, int column) {
    
  }
  
  public void getFileDelimiter() {
    // return char
  }
  
}

