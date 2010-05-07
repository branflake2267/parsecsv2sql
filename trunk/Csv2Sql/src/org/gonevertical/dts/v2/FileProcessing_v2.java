package org.gonevertical.dts.v2;

import java.io.File;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.gonevertical.dts.data.SourceData;
import org.gonevertical.dts.data.StatData;
import org.gonevertical.dts.lib.FileUtil;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;


public class FileProcessing_v2 {
	
	private Logger logger = Logger.getLogger(FileProcessing_v2.class);

  private TransformLib tl;
  
  private CsvProcessing_v2 csvProcess;

  // source data point
  private SourceData sourceData;
  
  // destination data point
  private DestinationData_v2 desinationData;

  private boolean returnToOptimise;

  /**
   * constructor
   */
  public FileProcessing_v2(SourceData sourceData, DestinationData_v2 destinationData) {
    this.sourceData = sourceData; 
    this.desinationData = destinationData;
    csvProcess = new CsvProcessing_v2(sourceData, destinationData);
    setSupportingLibraries();
  }
  
  /**
   * guice injects the libraries needed for the database
   */
  private void setSupportingLibraries() {
    tl = TransformLibFactory.getLib(desinationData.databaseData.getDatabaseType());
  }

  /**
   * run file processing
   */
  public void run() {

    // setup the files to process
    File[] files = null;
    if (sourceData.isFileDirectory() == true) { // is directory
      files = sourceData.file.listFiles();
      
    } else { // is file
      files = new File[1];
      files[0] = sourceData.file;
      
      if (sourceData.file.isFile() == false) {
      	logger.fatal("FileProcessing.run(): File is not a file; It has to be a valid directory or file. File: " + files[0].getName());
        //System.exit(1); // do not run this here b/c in a threaded pool, it shuts down everything
      	return;
      }
      
    }

    // work on files
    loop(files);

    logger.info("All done processing files for: sourceData.file: " + sourceData.file);
  }
  
  /**
   * loop through the files and import
   * 
   * @param files
   */
  private void loop(File[] files) {
  	
  	// warn if no files to work on
    notifyOnZeroFiles(files);
  	
  	if (files == null) {
  		return;
  	}

  	// sort files first
    Arrays.sort(files);
    
    int c = 0;
    for (int i=0; i < files.length; i++) {

      logger.info("Check if we can process this File/Directory: " + files[i].getName());

      if (skipFile(files[i]) == true) { // skip files we don't want like "~.file"
        logger.info("File is Registered to Skip: Skipping this file: " + files[i].getName());
        
      } else if (files[i].isFile() == true) { // process file
        
        // drop table, only on first file, if it is set
        if (c == 0) {
          dropTable();
        }
        
        // process this file
        csvProcess.processCsvFile(i, files[i]);
        
        // first time optimisation will cause this to happen
        if (csvProcess.getReturnToOptimise() == true) {
          returnToOptimise = true;
          return;
        }
        
        // move file to folder when done processing
        moveFileWhenDone(files[i]);
        
        c++;
      }
      
    } //end for

  }
  
  /**
   * warn there are no files to work on
   * 
   * @param files
   */
  private void notifyOnZeroFiles(File[] files) {
  	int count = countFilesInDirectory(files);
  	if (files != null && count > 0) {
  		return;
  	}
  	logger.error("FileProcessing.notifyOnZeroFiles(): Should you have a file to process? sourceData.file: " + sourceData.file);
  }

	/**
   * skip these files
   * 
   * @param file
   * @return
   */
  private boolean skipFile(File file) {
    boolean b = false;
    if (file == null) {
      b = true;
      
    // open office hidden file, which happens while looking at the csv file
    } else if (file.getName().toLowerCase().contains("~lock") == true) {
    	logger.info("Registered skip file: " + file.getName());
      b = true;
    }
    return b;
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
   * find data in a file
   * 
   * @return
   */
  public File findMatchInFile() {
        
    if (sourceData == null) {
      logger.fatal("SourceData not set. Exiting.");
      System.exit(1);
    }
    
    if (sourceData.ffsd == null) {
      logger.fatal("FlatFileSettings not set. Exiting.");
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
        logger.fatal("File is not a file; It has to be a valid directory or file. file:" + sourceData.file.toString());
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

      logger.info("Processing File: " + files[i].getName());

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
      logger.info("FileProcessing.dropTable(): Dropping Table");
      tl.dropTable(desinationData.databaseData, desinationData.table);
    }
  }
  
  public boolean getReturnOnOptimise() {
    boolean r = returnToOptimise;
    returnToOptimise = false;
    return r;
  }
	

  /**
   * count files that we can import
   * 
   * @param files
   * @return
   */
  private int countFilesInDirectory(File[] files) {
  	if (files == null) {
  		return 0;
  	}
  	
    int count = 0;
    
    for (int i=0; i < files.length; i++) {
      
    	if (files[i].isFile() && skipFile(files[i]) == false) {
        count++;
      }
    	
    }
    
    return count;
  }

	public StatData getStats() {
	  return csvProcess.getStats();
  }
  
}
