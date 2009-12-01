package org.gonevertical.csv2sql.lib.sql;

import static org.junit.Assert.*;

import org.gonevertical.csv2sql.data.ColumnData;
import org.gonevertical.csv2sql.data.DatabaseData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MsSqlQueryUtilTest_SqlEscape {

  private DatabaseData databaseData;

  @Before
  public void setUp() throws Exception {
    databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    // setup table to actually test inserting into
    ColumnData columnData = new ColumnData("test_escape", "Value", "VARCHAR(100) DEFAULT NULL");
    MySqlTransformUtil.createTable(databaseData, "test_escape", "TestId");
    MySqlTransformUtil.createColumn(databaseData, columnData);
  }

  @After
  public void tearDown() throws Exception {
    databaseData = null;
  }

  @Test
  public void testEscapeString1() {
    String value = "mystring\\";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }

  @Test
  public void testEscapeString2() {
    
    // TODO fix
    
    String value = "mystring\\\\ \\ "; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString3() {
    
    // TODO fix
    
    String value = "mystring\\\\ \\"; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString4() {
    String value = "value\\\\";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString5() {
    String value = "NELLA O\\\'NEAL";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString6() {
    String value = "MY\'S INTERNATIONAL"; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString7() {
    String value = "SAM O'NEILL JR";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(databaseData, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(databaseData, sql);
    assertEquals(value, valueTest);
  }
  

  
}
