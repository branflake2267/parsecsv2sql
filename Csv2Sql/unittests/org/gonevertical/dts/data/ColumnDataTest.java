package org.gonevertical.dts.data;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ColumnDataTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testColumnDataStringStringString() {
    String columnTable = "table";
    String columnName = "field";
    String columnType = "VARCHAR(25) DEFAULT NULL";
    ColumnData c = new ColumnData(columnTable, columnName, columnType);
    
    String rtable = c.getTable();
    String rname = c.getName();
    String rtype = c.getType();
    
    assertEquals(columnTable, rtable);
    assertEquals(columnName, rname);
    assertEquals(columnType.toLowerCase(), rtype);
  }

  @Test
  public void testSetValueString() {
    String columnTable = "table";
    String columnName = "field";
    String columnType = "VARCHAR(25) DEFAULT NULL";
    String value = "abcdefghijklmnopqrs";
    ColumnData c = new ColumnData(columnTable, columnName, columnType);
    c.setValue(value);
    String rvalue = c.getValue();
    assertEquals(value, rvalue);
    value = null;
    c.setValue(value);
    rvalue = c.getValue();
    assertEquals(value, rvalue);
    value = "";
    c.setValue(value);
    rvalue = c.getValue();
    assertEquals(null, rvalue);
  }

  @Test
  public void testSetValueLong() {
    String columnTable = "table";
    String columnName = "field";
    String columnType = "VARCHAR(25) DEFAULT NULL";
    long value = 1234567890;
    ColumnData c = new ColumnData(columnTable, columnName, columnType);
    c.setValue(value);
    long rvalue = Long.parseLong(c.getValue());
    assertEquals(value, rvalue);
  }

  @Test
  public void testGetValue() {
    // TODO - get value varchar
    // TODO - get value datetime
    // TODO - get value null
    // TODO - get value ""
    fail("Not yet implemented");
  }

  @Test
  public void testGetValueRaw() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetTestTypeThrow() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetValueAsInt() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetValueLength() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetName() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetColumnName() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetColumnAsSql() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetColumnAsSql() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetName() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetColumnName() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetIsPrimaryKey() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetIsPrimaryKey() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetOverwriteWhenBlank() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetOverwriteWhenBlank() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetOverwriteWhenZero() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetOverwriteWhenZero() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetRegex() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetRegex() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetTable() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetTable() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetCase() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetValueAsFunction() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetValueAsFunction() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsFunctionSetForValue() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetIdentity() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetIdentityUse() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetType() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetCharLengthString() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetType() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetCharLength() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetCharLengthInt() {
    fail("Not yet implemented");
  }

  @Test
  public void testDoesValueFitIntoColumn() {
    fail("Not yet implemented");
  }

  @Test
  public void testAlterColumnSizeBiggerIfNeedBe() {
    fail("Not yet implemented");
  }

  @Test
  public void testFixName() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetFieldType() {
    fail("Not yet implemented");
  }

  @Test
  public void testTestSizeOfValue() {
    fail("Not yet implemented");
  }

  @Test
  public void testTestSize_Text() {
    fail("Not yet implemented");
  }

  @Test
  public void testTestSize_Varchar() {
    fail("Not yet implemented");
  }

}
