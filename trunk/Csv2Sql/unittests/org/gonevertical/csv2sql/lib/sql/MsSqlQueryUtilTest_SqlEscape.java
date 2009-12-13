package org.gonevertical.csv2sql.lib.sql;

import static org.junit.Assert.assertEquals;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.MySqlQueryUtil;
import org.gonevertical.dts.lib.sql.MySqlTransformUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MsSqlQueryUtilTest_SqlEscape {

  private DatabaseData dd;

  @Before
  public void setUp() throws Exception {
    dd = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    // setup table to actually test inserting into
    ColumnData columnData = new ColumnData("test_escape", "Value", "VARCHAR(100) DEFAULT NULL");
    MySqlTransformUtil.createTable(dd, "test_escape", "TestId");
    MySqlTransformUtil.createColumn(dd, columnData);
    
    //String sql = "DELETE FROM test_escape;";
    //MySqlQueryUtil.update(dd, sql);
  }

  @After
  public void tearDown() throws Exception {
    dd = null;
  }

  @Test
  public void testEscapeString1() {
    String value = "mystring\\";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }

  @Test
  public void testEscapeString2() {
    String value = "mystring\\\\ \\ "; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString3() {
    String value = "mystring\\\\ \\"; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString4() {
    String value = "value\\\\";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString5() {
    String value = "MR O\\\'NEAL";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString6() {
    String value = "MY\'S INTERNATIONAL"; 
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString7() {
    String value = "MR O'NEILL JR";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString8() {
    String value = "single ' '' ''' '''' '''' '";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
  
  @Test
  public void testEscapeString9() {
    String value = "double \" \" \" \" \" \"";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }
}