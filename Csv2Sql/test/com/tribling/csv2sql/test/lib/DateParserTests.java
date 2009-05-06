package com.tribling.csv2sql.test.lib;

import com.tribling.csv2sql.lib.datetime.DateTimeParser;

public class DateParserTests {

  private DateTimeParser parse = new DateTimeParser();
  
  public DateParserTests() {
  }

  public void run() {
    
    parse.test_EngDate("3-apr-09", "04/03/2009");
    
  }
  
  
  
}
