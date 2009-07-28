package com.tribling.csv2sql.v2;

import java.io.File;
import java.util.Arrays;

import com.tribling.csv2sql.data.SourceData;
import com.tribling.csv2sql.lib.FileUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class FileProcessing_v2 {

  private CsvProcessing_v2 csvProcess = null;

  // source data point
  private SourceData sourceData = null;
  
  // destination data point
  private DestinationData_v2 desinationData = null;

  /**
   * constructor
   */
  public FileProcessing_v2(SourceData sourceData, DestinationData_v2 destinationData) {
    this.sourceData = sourceData; 
    this.desinationData = destinationData;
    csvProcess = new CsvProcessing_v2(sourceData, destinationData);
  }

  /**
   * run file processing
   */
  public void run() {

    // setup the files to process
    File[] files = null;
    if (sourceData.isFileDirectory() == true) {
      files = sourceData.file.listFiles();
      Arrays.sort(files);
    } else {
      files = new File[1];
      files[0] = sourceData.file;
      if (sourceData.file.isFile() == false) {
        System.err.println("File is not a file; It has to be a valid directory or file.");
        System.exit(1);
      }
    }

    loop(files);

    System.out.println("All Done: with files.");
  }
  
  /**
   * loop through the files and import
   * 
   * @param files
   */
  private void loop(File[] files) {

    Arrays.sort(files);

    for (int i=0; i < files.length; i++) {

      System.out.println("File: " + files[i].getName());

      if (files[i].isFile() == true) {
        
        // drop table, only on first file, if it is set
        if (i == 0) {
          dropTable();
        }
        // process this file
        csvProcess.parseFile(i, files[i]);
        
        // move file to folder when done processing
        moveFileWhenDone(files[i]);
      }
    }

  }
  
  /**
   * move file when done processing
   * 
   * @param file
   */
  private void moveFileWhenDone(File file) {
    if (desinationData != null && desinationData.moveFileToDone == true) {
      FileUtil f = new FileUtil();
      f.moveFileToFolder_Done(file);
    }
  }
  
  /**
   * how many real files are we going to process, this delegates the drop table
   * 
   * @param files
   * @return
   */
  private int howManyAreFiles(File[] files) {
    int is = 0;
    for (int i=0; i < files.length; i++) {
      if (files[i].isFile()) {
        is++;
      }
    }
    return is;
  }

  /**
   * find data in a file
   * 
   * @return
   */
  public File findMatchInFile() {
        
    if (sourceData == null) {
      System.out.println("SourceData not set. Exiting.");
      System.exit(1);
    }
    
    if (sourceData.ffsd == null) {
      System.out.println("FlatFileSettings not set. Exiting.");
      System.exit(1);
    }
    
    // set to match a file, not sql import
    csvProcess.setMode(FlatFileProcessing_v2.MODE_FINDFILEMATCH);
    
    File[] files = null;
    if (sourceData.isFileDirectory() == true) {
      files = sourceData.file.listFiles();
      Arrays.sort(files);
    } else {
      files = new File[1];
      files[0] = sourceData.file;
      if (sourceData.file.isFile() == false) {
        System.err.println("File is not a file; It has to be a valid directory or file.");
        System.exit(1);
      }
    }

    File foundFile = loop_ToFindMatch(files);
    
    return foundFile;
  }
  
  /**
   * loop throught the files and look for the match
   * 
   * @param files
   * @return
   */
  private File loop_ToFindMatch(File[] files) {

    Arrays.sort(files);

    File foundfile = null;
    for (int i=0; i < files.length; i++) {

      System.out.println("Processing File: " + files[i].getName());

      if (files[i].isFile() == true) {
        boolean found = csvProcess.parseFile_Match(i, files[i]);
        if (found == true) {
          foundfile = files[i];
          break;
        }
      }
    }

    return foundfile;
  }

  /**
   * drop table if it is set
   */
  private void dropTable() {
    if (desinationData.dropTable == true) {
      MySqlTransformUtil.dropTable(desinationData.databaseData, desinationData.table);
    }
  }
  
}
