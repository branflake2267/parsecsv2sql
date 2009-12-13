package org.gonevertical.dts.lib.sql.querylib;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.MySqlTransformLib;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MsSqlQueryLibTest {

  private DatabaseData dd;
  private QueryLib ql;
  private TransformLib tl;

  @Before
  public void setUp() throws Exception {
    dd = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    ql = QueryLibFactory.getLib(DatabaseData.TYPE_MYSQL);
    tl = TransformLibFactory.getLib(DatabaseData.TYPE_MYSQL);
  }

  @After
  public void tearDown() throws Exception {
    dd = null;
  }

  @Test
  public void testEscapeString() {
    String value = "\' \t lit\neral \\ mystring\\";
    String s = ql.escape(value);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = ql.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = ql.queryString(dd, sql);
    assertEquals(value, valueTest);
  }

  @Test
  public void testEscapeInt() {
    String s = ql.escape(1);
    String sql = "INSERT INTO test_escape SET Value='" + s + "'";
    long id = ql.update(dd, sql);
    sql = "SELECT Value FROM test_escape WHERE TestId='" + id + "';";
    String valueTest = ql.queryString(dd, sql);
    assertEquals(Integer.toString(1), valueTest);
  }

  @Test
  public void testGetResultSetSize() {
    String sql = "SELECT COUNT(*) AS t FROM test_escape;";
    int count = ql.queryInteger(dd, sql);
    sql = "SELECT * FROM test_escape;";
    int resultSize = 0;
    Connection conn = null;
    Statement select = null;
    try {
      conn = dd.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      resultSize = ql.queryInteger(dd, sql);
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: " + sql);
      e.printStackTrace();
    } finally {
      conn = null;
      select = null;
    }
    if (count == 0 | resultSize == 0) {
      count = -1;
    }
    assertEquals(count, resultSize);
  }

  @Test
  public void testQueryBoolean() {
    tl.dropTable(dd, "test_boolean");
    ColumnData columnData = new ColumnData("test_boolean", "Value", "BOOLEAN DEFAULT NULL");
    tl.createTable(dd, "test_boolean", "TestId");
    tl.createColumn(dd, columnData);
    
    String sql = "INSERT INTO test_boolean SET Value=1;";
    ql.update(dd, sql);
    
    Boolean b = ql.queryBoolean(dd, sql);
    
    assertEquals(b, true);
  }

  @Test
  public void testQueryInteger() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryLong() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryString() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryDouble() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryBigDecimal() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryIntegersToCsv() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryStringToCsv() {
    fail("Not yet implemented");
  }

  @Test
  public void testUpdateDatabaseDataString() {
    fail("Not yet implemented");
  }

  @Test
  public void testUpdateDatabaseDataStringBoolean() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryStringAndConvertToBoolean() {
    fail("Not yet implemented");
  }

  @Test
  public void testQueryLongAndConvertToBoolean() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetType() {
    fail("Not yet implemented");
  }

}
