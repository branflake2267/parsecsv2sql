package com.tribling.csv2sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;
import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;

public class CSVProcessing {

	// variables
	private File file;
	private char delimiter;
	private DestinationData desinationData;
	private MatchFieldData[] matchFields;
	
	
	// csv reader 2.0
	private CsvReader reader = null;
	
	// sql methods
	private OptimiseTable sql = new OptimiseTable();
	
	// the columns for the file
	private ColumnData[] columns;
	
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
	
	public void dropTableOff() {
		sql.dropTableOff();
	}
	
	public void closeConnection() {
		sql.closeConnection();
	}
	
	/** 
	 * start extracting the data
	 */
	protected void parseFile(int indexFile, File file) {
		this.file = file;
		
		// create table
		sql.createTable();
		
		// open the file
		readFile();
		
		// create columns
		try {
			getColumns();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// loop through data rows
		try {
			loopRowsData(indexFile);
		} catch (IOException e) {
			System.err.println("CSV Reader, Could not read data");
			e.printStackTrace();
		}
		
		// optimise table (if set on)
		sql.runOptimise(columns);
		
		// delete empty columns (if set on)
		sql.deleteEmptyColumns();
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
			reader.readHeaders();
			makeColumnlist(reader.getHeaders());
			System.out.println("column count: " + reader.getHeaderCount());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// insert/update columns
		if (columns != null) {
			this.columns = sql.createColumns(columns);
		} else {
			System.err.println("CSV Reader could not get columns");
			System.exit(1);
		}
		
	}
	
	private void loopRowsData(int indexFile) throws IOException {
		
		String[] values;
		int index = 0;
		while (reader.readRecord())
		{
			values = reader.getValues();
			
			// add data to table
			sql.addData(indexFile, index, columns, values);
			
			// debug
			//listValues(index, values);
			
			index++;
		}
		
	}
	
	private void listValues(int index, String[] values) {
		
		String s = "";
		for (int i=0; i < values.length; i++) {
			s += ", " + values[i];
		}
		
		System.out.println(index + ". " + s);
	}
	
	private void makeColumnlist(String[] columns) {
		ColumnData[] cols = new ColumnData[columns.length];
		for(int i=0; i < columns.length; i++) {
			cols[i] = new ColumnData();
			cols[i].column = columns[i];
		}
		this.columns = cols;
	}
	
	
	
	
}
