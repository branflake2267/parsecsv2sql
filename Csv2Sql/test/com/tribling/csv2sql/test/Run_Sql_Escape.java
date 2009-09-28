package com.tribling.csv2sql.test;

import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;

public class Run_Sql_Escape {

  public static void main(String[] args) {
    
    String value = "mystring\\";
   
    String s = MySqlQueryUtil.escape(value);
    
    System.out.println("s: " + s);
  }
  
}
