package com.tribling.csv2sql;

import java.beans.DesignMode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;

public class CSVProcessing {

	// variables
	private File file;
	private char delimiter;
	private DestinationData desinationData;
	private MatchFieldData[] matchFields;
	
	
	// csv reader 2.0
	private CsvReader reader;
	
	// sql methods
	private OptimiseTable sql = new OptimiseTable();
	
	private String[] columns;
	
	/**
	 * constructor
	 */
	public CSVProcessing() {
	}
	
	protected void setData(char delimiter, DestinationData destinationData, MatchFieldData[] matchFields) {
		this.delimiter = delimiter;
		this.file = file;
		this.desinationData = destinationData;
		this.matchFields = matchFields;
		
		try {
			sql.setDestinationData(destinationData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		sql.setMatchFields(matchFields);
	}
	
	/** 
	 * start extracting the data
	 */
	protected void parseFile(File file) {
		this.file = file;
		
		readFile();
		
		try {
			getColumns();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			loopRowsData();
		} catch (IOException e) {
			System.err.println("CSV Reader, Could not read data");
			e.printStackTrace();
		}
		
		// TODO - Optimise it
	}
	
	private void readFile() {
		try {
			reader = new CsvReader(file.toString(), delimiter);
		} catch (FileNotFoundException e) {
			System.err.println("CSV Reader, Could not open CSV Reader");
			e.printStackTrace();
		}
	}
	
	private void getColumns() throws Exception {
				
		try {
			columns = reader.getHeaders();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// insert/update columns
		if (columns != null) {
			sql.createColumns(columns);
		} else {
			System.err.println("CSV Reader could not get columns");
			throw new Exception();
		}
		
	}
	
	private void loopRowsData() throws IOException {
		
		String[] values;
		while (reader.readRecord())
		{
			values = reader.getValues();
			
			// add data to table
			sql.addData(columns, values);
		}
		
	}
	
}
