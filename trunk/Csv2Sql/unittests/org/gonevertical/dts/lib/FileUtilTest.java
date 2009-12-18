package org.gonevertical.dts.lib;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URISyntaxException;

import org.gonevertical.dts.test.Run_Test_Import_v1;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileUtilTest {

  private FileUtil fileUtil = null;
  
  private String execPath = "";
  
  @Before
  public void setUp() throws Exception {
    fileUtil = new FileUtil();
    
    File executionlocation = null;
    try {
      executionlocation = new File(Run_Test_Import_v1.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    execPath = executionlocation.getParent();
  }

  @After
  public void tearDown() throws Exception {
    fileUtil = null;
    execPath = null;
  }

  @Test
  public void testGetFileLineCount() {
    int linecount = fileUtil.getFileLineCount(new File(execPath + "/data/test/linecount.txt"));
    assertEquals(10, linecount);
  }

  @Test
  public void testFindInFile() {
    fail("Not yet implemented");
  }

  @Test
  public void testReplaceInFile() {
    fail("Not yet implemented");
  }

  @Test
  public void testFindInDir() {
    fail("Not yet implemented");
  }

  @Test
  public void testFindLineCount() {
    fail("Not yet implemented");
  }

  @Test
  public void testMoveFileStringString() {
    fail("Not yet implemented");
  }

  @Test
  public void testMoveFileFileString() {
    fail("Not yet implemented");
  }

  @Test
  public void testMoveFileFileFile() {
    fail("Not yet implemented");
  }

  @Test
  public void testCreateDirectory() {
    fail("Not yet implemented");
  }

  @Test
  public void testMoveFileToFolder_Done() {
    fail("Not yet implemented");
  }

  @Test
  public void testDoesFileHeaderMatchStr() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetHeader() {
    fail("Not yet implemented");
  }

  @Test
  public void testDoesFileNameMatch() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetFileSize() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetNewFileName() {
    fail("Not yet implemented");
  }

}
