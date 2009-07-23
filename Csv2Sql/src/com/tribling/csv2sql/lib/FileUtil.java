package com.tribling.csv2sql.lib;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.csvreader.CsvReader;

public class FileUtil {

  public FileUtil() {
  }
  
  /**
   * how many lines in the file, good for figuring out how many csv rows
   * 
   * @param file
   * @return
   */
  public int getFileLineCount(File file) {
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    DataInputStream dis = null;

    int i = 0;
    try {
      fis = new FileInputStream(file);

      // Here BufferedInputStream is added for fast reading.
      bis = new BufferedInputStream(fis);
      dis = new DataInputStream(bis);

      // dis.available() returns 0 if the file does not have more lines.
      while (dis.available() != 0) {
        dis.readLine();
        //System.out.println(dis.readLine());
        i++;
      }

      // dispose all the resources after using them.
      fis.close();
      bis.close();
      dis.close();

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return i;
  }
  
  /**
   * find regex value in a file
   * 
   * @param file
   * @param regex
   * @return
   */
  public boolean findInFile(File file, String regex) {
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    DataInputStream dis = null;
    boolean found = false;
    try {
      fis = new FileInputStream(file);
      bis = new BufferedInputStream(fis);
      dis = new DataInputStream(bis);
      while (dis.available() != 0) {
        // TODO - change method to get line
        String s = dis.readLine();
        found = StringUtil.findMatch(regex, s);
        if (found == true) {
          break;
        }
      }
      fis.close();
      bis.close();
      dis.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return found;
  }
 
  /**
   * find a file with a regex value
   * 
   * @param dir
   * @param regex
   * @return
   */
  public File findInDir(File dir, String regex) {
    
    if (dir.isDirectory() == false) {
      return null;
    }
    
    File[] files = dir.listFiles();
    
    File foundFile = null;
    for (int i=0; i < files.length; i++) {
      boolean found = findInFile(files[i], regex);
      if (found == true) {
        foundFile = files[i];
        break;
      }
    }
    
    return foundFile;
  }
  
  /**
   * find a line count of a file with a particular regex value in a file
   * 
   * @param dir
   * @param regex
   * @return
   */
  public int findLineCount(File dir, String regex) {
   
    // first find the file we want
    File foundFile = findInDir(dir, regex);
    
    if (foundFile == null) {
      System.out.println("findLineCount: couldn't find file");
      return 0;
    }
    
    // now get the line count
    int linecount = getFileLineCount(foundFile);
    
    return linecount;
  }
  
  /**
   * move file to new directory, and it will change the file name if one exists already
   * 
   * @param moveFile
   * @param toDir
   */
  public void moveFile(String moveFile, String toDir) {
    moveFile(new File(moveFile), new File(toDir));
  }
 
  public void moveFile(File moveFile, String toDir) {
    moveFile(moveFile, new File(toDir));
  }
  
  public void moveFile(File moveFile, File toDir) {
    
    if (toDir.exists() == false) {
      toDir.mkdirs();
    }
    
    File checkFile = new File(toDir.getPath() + "/" + moveFile.getName());
    String f = "";
    if (checkFile.exists() == true) {
      int i = getFileCount(toDir);
      f = i + "_";
    } 
    
    File file = moveFile;
    File dir = toDir;
    file.renameTo(new File(dir, f + file.getName()));
  }
  
  private int getFileCount(File dir) {
    int i = 0;
    File[] files = dir.listFiles();
    if (files != null) {
      i = files.length;
    }
    return i;
  }
  
  public void createDirectory(String path) {
    
    if (path == null && path.length() == 0) {
      System.out.println("createDirectory path was null");
      return;
    }
    
    File file = new File(path);
    file.mkdirs();
  }
  
  /**
   * move this file to a done folder
   * 
   * @param file
   */
  public void moveFileToFolder_Done(File file) {
    
    // create done folder if it doesn't exist
    String donePath = file.getParent() + "/done"; 
    createDirectory(donePath);
    
    moveFile(file, donePath);
  }
  
  /**
   * match a file with the same header values given
   * 
   * @param file
   * @param matchHeaderValues
   * @param delimiter
   * @return
   */
  public boolean doesFileHeaderMatchStr(File file, String matchHeaderValues, char delimiter) {
    
    // match with out a delimiter
    String sdelimiter = Character.toString(delimiter);
    matchHeaderValues = matchHeaderValues.replaceAll(sdelimiter, "");

    
    CsvReader reader = null;
    try {     
      reader = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("doesFileHeaderMatchStr: Could not open CSV Reader");
      e.printStackTrace();
    }
    
    if (reader == null) {
      return false;
    }
    
    String[] header = null;
    try {
      reader.readHeaders();
      header = reader.getHeaders();
    } catch (IOException e) {
      System.out.println("doesFileHeaderMatchStr: could not read headers");
      e.printStackTrace();
    }
    
    if (header == null) {
      return false;
    }
    
    String sheader = "";
    for (int i=0; i < header.length; i++) {
      sheader += header[i];
    }
    
    if (sheader.equals(matchHeaderValues)) {
      return true;
    }
    
    // TODO - do a lessor, use less file values to make string and check ??
    
    return false;
  }
  
  /**
   * get csv header fields
   * 
   * @param file
   * @param delimiter
   * @return
   */
  public String[] getHeader(File file, char delimiter) {
    
    CsvReader reader = null;
    try {     
      reader = new CsvReader(file.toString(), delimiter);
    } catch (FileNotFoundException e) {
      System.err.println("doesFileHeaderMatchStr: Could not open CSV Reader");
      e.printStackTrace();
    }
    
    if (reader == null) {
      return null;
    }
    
    String[] header = null;
    try {
      reader.readHeaders();
      header = reader.getHeaders();
    } catch (IOException e) {
      System.out.println("doesFileHeaderMatchStr: could not read headers");
      e.printStackTrace();
    }
    
    if (header == null) {
      return null;
    }
    
    return header;
  }
  
  public boolean doesFileNameMatch(File file, String regex) {
    
    if (regex == null | file.getName() == null) {
      return false;
    }
    
    boolean b = false;
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(file.getName());
      b = m.find();
    } catch (Exception e) {
      System.out.println("doesFileNameMatch: regex error");
    }
    
    return b;
  }
  
  
  
}
