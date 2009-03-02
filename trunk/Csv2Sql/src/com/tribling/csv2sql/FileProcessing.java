package com.tribling.csv2sql;

import java.io.File;
import java.util.Arrays;

import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class FileProcessing {
	
	private CSVProcessing csvProcess = new CSVProcessing();
	
	// sql items
	private DestinationData desinationData = null;
	
	private MatchFieldData[] matchFields = null;

	private char delimiter;
	
	private boolean isDirectory = false;
	
	/**
	 * constructor
	 */
	public FileProcessing() {
	}
	
	/**
	 * set the data, and it will run
	 * 
	 * @param sourceData
	 * @param destinationData
	 * @param matchFields
	 */
	public void setData(SourceData sourceData, DestinationData destinationData, MatchFieldData[] matchFields) {
		this.delimiter = sourceData.delimiter;
		this.desinationData = destinationData;
		
		if (matchFields != null) {
			Arrays.sort(matchFields);
		}
		this.matchFields = matchFields;
		
		csvProcess.setData(delimiter, destinationData, matchFields);
		
		try {
			run(sourceData);
		} catch (Exception e) {
			System.out.println("File Error:");
			e.printStackTrace();
		}
	}
	
	/**
	 * start running the process
	 * 
	 * @param sourceData
	 * @throws Exception 
	 */
	private void run(SourceData sourceData) {
	
		File[] files;
		
		// is the file  a file or directory
		isDirectory = sourceData.file.isDirectory();
		if (isDirectory == true) {
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
	 * start the loop through the files
	 * 
	 * @param files
	 */
	private void loop(File[] files) {
	
		Arrays.sort(files);
		
		for (int i=0; i < files.length; i++) {
			
			System.out.println("File: " + files[i].getName());
			
			if (files[i].isFile() == true) {
				
				// when extracting a bunch of the same files, skip optimisation after the first
				if (isDirectory == true && i > 0 && howManyAreFiles(files) > 1) {
					csvProcess.dropTableOff();
				}
				
				csvProcess.parseFile(i, files[i]);
			}
		}

	}

	/**
	 * how many real files are we going to process, this delegates the drop table
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
	
}
