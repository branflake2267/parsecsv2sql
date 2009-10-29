package com.tribling.csv2sql.test;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class Run_Sql_Escape {

  public static void main(String[] args) {
    
    DatabaseData databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    ColumnData columnData = new ColumnData("test_escape", "Value", "VARCHAR(100) DEFAULT NULL");
    
    MySqlTransformUtil.createTable(databaseData, "test_escape", "TestId");
    MySqlTransformUtil.createColumn(databaseData, columnData);
    
    
    // 1
    String value = "mystring\\";
    String s = MySqlQueryUtil.escape(value);
    System.out.println("s: " + s);
    
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    MySqlQueryUtil.update(databaseData, sql);
    
    
    
    // 2
    value = "mystring\\\\ \\ "; 
    s = MySqlQueryUtil.escape(value);
    System.out.println("s: " + s);
    
    sql = "INSERT INTO test_escape SET Value='" + s + "'";
    MySqlQueryUtil.update(databaseData, sql);
    
    
    
    // 3
    value = "mystring\\\\ \\"; 
    s = MySqlQueryUtil.escape(value);
    System.out.println("s: " + s);
    
    sql = "INSERT INTO test_escape SET Value='" + s + "'";
    MySqlQueryUtil.update(databaseData, sql);
  }
  
}
