package com.tribling.csv2sql.test;

import com.tribling.csv2sql.lib.datetime.DateTimeParser;

public class Run_TestDateParser_Single {

  public static void main(String[] args) {
    
    DateTimeParser parse = new DateTimeParser();
    
    parse.test_MySqlDate("8/6/2009 9:42", "2009-08-06 09:42:00"); 
  }
  
}
