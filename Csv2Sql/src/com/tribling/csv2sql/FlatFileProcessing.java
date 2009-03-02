package com.tribling.csv2sql;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tribling.csv2sql.data.FlatFileSettingsData;

/**
 * process the flat file before going into sql
 * 
 * TODO Options, only work on a row, only work on a column, only work on a row and column...
 *
 * 
 * @author branflake2267
 *
 */
public class FlatFileProcessing {

  // set all the settings into this object array
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
  public FlatFileProcessing() {
  }
  
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
  public void setautoFormatValue(int row, int column) {
    
  }
   
  /**
   * get the files delimiter by sampling the the files
   */
  public void getFileDelimiter() {
    // return char
  }
  
  public void setsampleColumnType(int row, int column) {
    
  }
  
  /**
   * are there any settings set, so then to evaluate the column?
   * @return
   */
  protected boolean hasSettings() {
    boolean b = false;
    if (settings.size() > 0) {
      b = true;
    }
    return b;
  }
  
  protected String evaluateColumn(int row, int column, String value) {
    
    if (settings.size() == 0) {
      return value;
    }
  
    for (int i=0; i < settings.size(); i++) {
      
      if (doesffsd_match(settings.get(i), row, column) == true) {
        changeIt(settings.get(i), value);
      }
      
    }
    
    return null;
  }
  
  /**
   * does the row and column match somethign that was set?
   * 
   * @param ffsd
   * @param row
   * @param column
   * @return
   */
  private boolean doesffsd_match(FlatFileSettingsData ffsd, int row, int column) {
    boolean b = false;
    if (row > 0 && column > 0 && ffsd.row == row && ffsd.column == column) { // matches both row and column
      b = true;
    } else if (row > 0 && column == 0 && ffsd.row == row) { // matches the row
      b = true;
    } else if (row == 0 && column > 0 && ffsd.column == column) { // matches the column 
      b = true;
    }
    return b;
  }
  
  private String changeIt(FlatFileSettingsData ffsd, String value) {
    
    String v = null;
    switch (ffsd.action) {
    case CHANGEVALUE:
      v = ffsd.value;
      break;
    case CHANGEVALUEWHENEMPTY:
      changeEmpty(ffsd, value);
      break;
    case CHANGEVALUEWITHREGEXMATCH:
      changeRegexMatch(ffsd, value);
      break;
    case CHANGEVALUEWITHREGEXREPLACE:
      changeRegexReplace(ffsd, value);
      break;
    case CHANGEVALUEAUTO:
      break;
      
    default:
      v = value;
      break;
    }
    
    return v;
  }
  
  private String changeEmpty(FlatFileSettingsData ffsd, String value) {
    
    if (value == null) {
      value = "";
    }
    
    if (value.length() == 0) {
      value = ffsd.value;
    }
    
    return value;
  }
  
  private String changeRegexMatch(FlatFileSettingsData ffsd, String value) {
    
    if (value == null) {
      value = "";
    }
    
    if (ffsd.regex == null) {
      return value;
    }
    
    if (value.matches(ffsd.regex) == true) {
      value = ffsd.value;
    }
    
    return value;
  }
  
  /**
   * replace with regex 
   * 
   * TODO - this can be enhanced
   * 
   * @param ffsd
   * @param value
   * @return
   */
  private String changeRegexReplace(FlatFileSettingsData ffsd, String value) {
    
    String re = ffsd.regex;
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    boolean found = m.find();

    String v = "";
    if (found == true) {
      v = m.group(1);
      // TODO - get more groups and stick in string?
    }
    
    return v;
  }
  
}

