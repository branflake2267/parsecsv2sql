package org.gonevertical.dts.test;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.MySqlQueryUtil;
import org.gonevertical.dts.lib.sql.MySqlTransformUtil;


public class Run_Sql_Escape {

  public static void main(String[] args) {
    
    DatabaseData databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    ColumnData columnData = new ColumnData("test_escape", "Value", "VARCHAR(100) DEFAULT NULL");
    
    MySqlTransformUtil.createTable(databaseData, "test_escape", "TestId");
    MySqlTransformUtil.createColumn(databaseData, columnData);
    
    String value = "mystring\\\\ \\ "; 
    
    String s = MySqlQueryUtil.escape(value);
    
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    
    System.out.println("sql: " + sql);
    
    long id = MySqlQueryUtil.update(databaseData, sql);
    
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    
    
    
    
    
    System.out.println("value: " + value + " valueTest: " + valueTest);
    
    
  }
  
}
