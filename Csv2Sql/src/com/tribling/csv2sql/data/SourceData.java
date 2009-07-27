package com.tribling.csv2sql.data;

import java.io.File;
import java.io.FileNotFoundException;

import com.csvreader.CsvReader;

public class SourceData {
 
	// file or directory
	public File file;
	
	// column/field separator
	public char delimiter;
	
	// flate file pre-processing
	public FlatFileSettingsData ffsd = null;
	
	/**
	 * constructor
	 */
	public SourceData() {
	}
	
	/**
	 * is the source file a directory?
	 * @return
	 */
	public boolean isFileDirectory() {
	  boolean b = file.isDirectory();
	  return b;
	}
	

	
}
