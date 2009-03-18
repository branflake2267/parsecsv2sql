package com.tribling.csv2sql;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tribling.csv2sql.data.FlatFileSettingsData;
import com.tribling.csv2sql.datetime.DateTimeParser;

/**
 * process the flat file before going into sql
 * 
 * Options, only work on a row, only work on a column, only work on a row and column...
 *
 * 
 * @author branflake2267
 *
 */
public class FlatFileProcessing {

  //set all the settings into this object array
  private ArrayList<FlatFileSettingsData> settings = new ArrayList<FlatFileSettingsData>();
  
  /**
   * constructor
   */
  public FlatFileProcessing() {
  }
  
  protected void setData(FlatFileSettingsData ffsd) {
    this.settings = ffsd.getSettings();
  }
  
  /**
   * get the files delimiter by sampling the the files
   */
  public void getFileDelimiter() {
    // return char
  }
  

  private void setsampleColumnType(int row, int column) {
    
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
  
  protected String evaluate(int row, int column, String value) {
    
    if (settings.size() == 0) {
      return value;
    }
  
    // loop through settings
    for (int i=0; i < settings.size(); i++) {
      
      // does a setting match row,column
      if (doesffsd_match(settings.get(i), row, column, value) == true) {
      
        // change csv value if matched
        value = changeIt(settings.get(i), value);
      
      }
      
    }
    
    return value;
  }
  
  /**
   * does the row and column match something that was set?
   * 
   * @param ffsd
   * @param row
   * @param column
   * @return
   */
  private boolean doesffsd_match(FlatFileSettingsData ffsd, int row, int column, String value) {
    
    // only for header row - auto change row/column for future row matching for column that header was matched in
    if (ffsd.headerField != null && row == 0) {
      
      // TODO - not sure if this reference will work to set the vars
      if (ffsd.headerField.equals(value)) { // NOTE: matching "" could be bad?
        ffsd.row = -1;
        ffsd.column = column; // this is the column that the header field is in
        return false;
      }
      
    }
    
    boolean b = false;
    if (ffsd.row == row && ffsd.column == column) { // matches both row and column
      b = true;
      
    } else if (ffsd.row == row && ffsd.column == -1) { // matches the row
      b = true;
      
    } else if (ffsd.column == column && ffsd.row == -1) { // matches the column 
      b = true;
    }
    
    return b;
  }
  
  /**
   * change the value b/c it met the row/column criteria, now goto this step
   * 
   * @param ffsd
   * @param value
   * @return
   */
  private String changeIt(FlatFileSettingsData ffsd, String value) {
    
    String v = null;
    switch (ffsd.action) {
    case FlatFileSettingsData.CHANGEVALUE:
      v = ffsd.value;
      break;
    case FlatFileSettingsData.CHANGEVALUEWHENEMPTY:
      v = changeEmpty(ffsd, value);
      break;
    case FlatFileSettingsData.CHANGEVALUEWITHREGEXMATCH:
      v = changeRegexMatch(ffsd, value);
      break;
    case FlatFileSettingsData.CHANGEVALUEWITHREGEXREPLACE:
      v = changeRegexReplace(ffsd, value);
      break;
    case FlatFileSettingsData.CHANGEVALUEAUTO:
      // TODO - do later in the future
      v = "";
      break;
    case FlatFileSettingsData.CHANGEVALUEINTODATESTRING:
      v = changeValueToDateFormat(ffsd, 1, value);
      break;
    case FlatFileSettingsData.CHANGEVALUEINTODATETIME:
      v = changeValueToDateFormat(ffsd, 2, value);
      break;
    default:
      v = value;
      break;
    }
    
    return v;
  }
  
  /**
   * change value when empty
   * 
   * @param ffsd
   * @param value
   * @return
   */
  private String changeEmpty(FlatFileSettingsData ffsd, String value) {
    
    if (value == null) {
      value = "";
    }
    
    if (value.length() == 0) {
      value = ffsd.value;
    }
    
    return value;
  }
  
  /**
   * change value with regex matching
   * 
   * @param ffsd
   * @param value
   * @return
   */
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
   * replace with regex upon regex match of that value
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
    } else {
      v = value;
    }
    
    return v;
  }
  
  private String changeValueToDateFormat(FlatFileSettingsData ffsd, int formatType, String value) {
    
    DateTimeParser parse = new DateTimeParser();
    
    if (formatType == 1) {
      value = parse.getDate_EngString(value);
    } else if (formatType == 2){
      value = parse.getDateMysql(value);
    }
    
    return value;
  }
  

  
}

