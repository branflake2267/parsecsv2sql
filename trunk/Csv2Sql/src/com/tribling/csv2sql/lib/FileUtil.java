package com.tribling.csv2sql.lib;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

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
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}
