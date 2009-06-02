package com.tribling.csv2sql.lib.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import com.tribling.csv2sql.lib.FileUtil;

public class MoveFile extends FileUtil {

  // what to match and move to where
  private ArrayList<MoveFileData> moveFileData = null;
  
  // diretory to process
  private File dir = null;
  
  
  public MoveFile() {
    moveFileData = new ArrayList<MoveFileData>();
  }
  
  public void addMoveFileData(MoveFileData moveFileData) {
    this.moveFileData.add(moveFileData);
  }
  
  public void setProcessDirectory(File dir) {
    this.dir = dir;
  }
  
  public void run() {
    processFiles();
  }
  
  private void processFiles() {
    
    File[] files = dir.listFiles();
    Arrays.sort(files);
    
    for (int i=0; i < files.length; i++) {
      System.out.println("processing file: " + files[i].getName());
      check(files[i]);
    }
    
  }

  private void check(File file) {
   
    for (int i=0; i < moveFileData.size(); i++) {
   
      String matchHeaderValues = moveFileData.get(i).matchHeaderValues;
      char delimiter = moveFileData.get(i).delimiter;
      String toDir = moveFileData.get(i).pathToMoveToDir;
      
      if (doesFileHeaderMatchStr(file, matchHeaderValues, delimiter) == true) {
        System.out.println("\tmoving file:" + file.getName());
        moveFile(file, toDir);
        break;
      }
      
    }
    
  }
  
}
