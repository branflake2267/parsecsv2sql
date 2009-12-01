package org.gonevertical.csv2sql.lib.sql;

import static org.junit.Assert.*;

import org.gonevertical.csv2sql.data.ColumnData;
import org.gonevertical.csv2sql.data.DatabaseData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MsSqlQueryUtilTest {

  private DatabaseData databaseData;

  @Before
  public void setUp() throws Exception {
    databaseData = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
  }

  @After
  public void tearDown() throws Exception {
    databaseData = null;
  }

  @Test
  public void testEscapeString() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testEscapeInt() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public void testGetResultSetSize() {
    fail("Not yet implemented"); // TODO
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
