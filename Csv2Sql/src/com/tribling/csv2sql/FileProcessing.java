package com.tribling.csv2sql;

import java.io.File;
import java.util.Arrays;

import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class FileProcessing {
	
	private CSVProcessing csv = new CSVProcessing();
	
	// sql items
	private DestinationData desinationData;
	private MatchFieldData[] matchFields;

	private char delimiter;
	
	private boolean isDirectory = false;
	
	/**
	 * constructor
	 */
	public FileProcessing() {
	}
	
	/**
	 * set the data, and it will run
	 * @param sourceData
	 * @param destinationData
	 * @param matchFields
	 */
	public void setData(SourceData sourceData, DestinationData destinationData, MatchFieldData[] matchFields) {
		this.delimiter = sourceData.delimiter;
		this.desinationData = destinationData;
		
		Arrays.sort(matchFields);
		this.matchFields = matchFields;
		
		csv.setData(delimiter, destinationData, matchFields);
		
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
	private void run(SourceData sourceData) throws Exception {
	
		File[] files;
		
		// is the file  a file or directory
		isDirectory = sourceData.file.isDirectory();
		if (isDirectory == true) {
			files = sourceData.file.listFiles();
		} else {
			files = new File[1];
			files[0] = sourceData.file;
			if (sourceData.file.isFile() == false) {
				System.err.println("File is not a file; It has to be a valid directory or file.");
				throw new Exception();
			}
		}
		
		loop(files);
		
		System.out.println("All Done");
	}
	
	/**
	 * start the loop through the files
	 * 
	 * @param files
	 */
	private void loop(File[] files) {
	
		for (int i=0; i < files.length; i++) {
			if (files[i].isFile() == true) {
				
				// when extracting a bunch of the same files, skip optimisation after the first
				if (isDirectory == true && i > 0 && howManyAreFiles(files) > 1) {
					csv.dropTableOff();
				}
				
				csv.parseFile(i, files[i]);

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
