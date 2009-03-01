package com.tribling.csv2sql.data;

public class FlatFileSettingsData {

  // work on options logic
  // on row
  // on column
  // on both row + column
  
  // what action to take when this matches
  public int action = 0;
  
  // work on row x
  public int row = 0;
  
  // work on col y 
  public int column = 0;
  
  // match or replace regex
  public String regex = null;
  
  // value to replace with match regex
  public String value = null;
  
}
