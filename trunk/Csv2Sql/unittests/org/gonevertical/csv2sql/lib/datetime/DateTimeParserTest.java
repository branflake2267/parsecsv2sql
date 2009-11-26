package org.gonevertical.csv2sql.lib.datetime;


import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DateTimeParserTest {

  private DateTimeParser dtp;

  @Before
  public void setUp() throws Exception {
    dtp = new DateTimeParser();
  }

  @After
  public void tearDown() throws Exception {
    dtp = null;
  }

  @Test
  public void simpleAdd() {
      int result = 1;
      int expected = 1;
      assertEquals(result, expected);
  }
  
  @Test
  public void test1() { 
    assertEquals(dtp.getDateMysql("04/01/2008"), "2008-04-01 00:00:00");
  } 
  
}
