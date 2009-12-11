package org.gonevertical.csv2sql.lib.sql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gonevertical.csv2sql.data.ColumnData;
import org.gonevertical.csv2sql.data.DatabaseData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MsSqlQueryUtilTest {

  private DatabaseData dd;

  @Before
  public void setUp() throws Exception {
    dd = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
  }

  @After
  public void tearDown() throws Exception {
    dd = null;
  }

  @Test
  public void testEscapeString() {
    String value = "mystring\\";
    String s = MySqlQueryUtil.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(value, valueTest);
  }

  @Test
  public void testEscapeInt() {
    String s = MySqlQueryUtil.escape(1);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = MySqlQueryUtil.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = MySqlQueryUtil.queryString(dd, sql);
    assertEquals(Integer.toString(1), valueTest);
  }

  @Test
  public void testGetResultSetSize() {
    String sql = "SELECT COUNT(*) AS t FROM test_escape;";
    int count = MySqlQueryUtil.queryInteger(dd, sql);
    sql = "SELECT * FROM test_escape;";
    int resultSize = 0;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      resultSize = MySqlQueryUtil.queryInteger(dd, sql);
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: " + sql);
      e.printStackTrace();
    } 
    if (count == 0 | resultSize == 0) {
      count = -1;
    }
    assertEquals(count, resultSize);
  }

  @Test
  public void testQueryBoolean() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryInteger() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryLong() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryString() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryDouble() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryBigDecimal() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryIntegersToCsv() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryStringToCsv() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testUpdateDatabaseDataString() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testUpdateDatabaseDataStringBoolean() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryStringAndConvertToBoolean() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testQueryLongAndConvertToBoolean() {
    fail("Not yet implemented"); // TODO
  }

}