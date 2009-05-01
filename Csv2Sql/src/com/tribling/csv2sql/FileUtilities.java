package com.tribling.csv2sql;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileUtilities {

  public FileUtilities() {
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
  
  public boolean findInFile(File file, String s) {
    
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
        String line = dis.readLine();
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
    
    return false;
  }
 
  private boolean findMatch(String match, String s) {
    
    return false;
  }
  
}
