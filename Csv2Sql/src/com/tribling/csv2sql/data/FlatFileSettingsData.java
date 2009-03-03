package com.tribling.csv2sql.data;

import java.util.ArrayList;

public class FlatFileSettingsData {

  //set all the settings into this object array
  private ArrayList<FlatFileSettingsData> settings = new ArrayList<FlatFileSettingsData>();
  
  public final static int CHANGEVALUE = 1;
  
  public final static int CHANGEVALUEWHENEMPTY = 2;
  
  public final static int CHANGEVALUEWITHREGEXMATCH = 3;
  
  public final static int CHANGEVALUEWITHREGEXREPLACE = 4;
  
  public final static int CHANGEVALUEAUTO = 5;
  
  // english date format 01/31/2009
  public final static int CHANGEVALUEINTODATESTRING = 6;
  
  // sql datetime format
  // TODO logic for the sql db needed in dateparser class
  public final static int CHANGEVALUEINTODATETIME = 7;
  
  /**
   * constructor
   */
  public FlatFileSettingsData() {
  }

  /**
   * get the settings data for processing
   * 
   * @return
   */
  public ArrayList<FlatFileSettingsData> getSettings() {
    return settings;
  }

  // work on options logic
  // on row
  // on column
  // on both row + column
  
  // what action to take when this matches
  public int action = 0;
  
  // work on row x
  public int row = -1;
  
  // work on col y 
  public int column = -1;
  
  // match or replace regex
  public String regex = null;
  
  // value to replace with match regex
  public String value = null;
  
  
  /* TODO add more options, that is just row, or just column for all types
   * TODO -1 is that of don't match column or row
   * */
  
  public void setchangeValue(int row, int column, String value) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUE;
    ffsd.row = row;
    ffsd.column = column;
    ffsd.value = value;
    settings.add(ffsd);
  }
  
  public void setchangeValueWhenEmptyMatch(int row, int column, String value) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUEWHENEMPTY;
    ffsd.row = row;
    ffsd.column = column;
    ffsd.value = value;
    settings.add(ffsd);
  }
  
  public void setchangeValueWithRegexMatch(int row, int column, String regex, String value) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUEWITHREGEXMATCH;
    ffsd.row = row;
    ffsd.column = column;
    ffsd.value = value;
    ffsd.regex = regex;
    settings.add(ffsd);
  }
  
  public void setchangeValueWithRegexReplace(int row, int column, String regex) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUEWITHREGEXREPLACE;
    ffsd.row = row;
    ffsd.column = column;
    ffsd.regex = regex;
    settings.add(ffsd);
  }
  
  public void setchangeValueIntoDateStringFormat(int row, int column) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUEINTODATESTRING;
    ffsd.row = row;
    ffsd.column = column;
    settings.add(ffsd);
  }
  
  public void setchangeValueIntoSqlDateTimeFormat(int row, int column) {
    FlatFileSettingsData ffsd = new FlatFileSettingsData();
    ffsd.action = CHANGEVALUEINTODATETIME;
    ffsd.row = row;
    ffsd.column = column;
    settings.add(ffsd);
  }

  /**
   * auto format a to best values, like date and phone numbers to consistent format 
   * 
   * TODO date format like ? datetime or common eng
   * @param row
   * @param column
   */
  private void setautoFormatValue(int row, int column) {
    
  }
  
}
