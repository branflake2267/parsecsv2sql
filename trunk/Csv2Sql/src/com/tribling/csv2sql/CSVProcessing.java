package com.tribling.csv2sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.csvreader.CsvReader;
import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;

public class CSVProcessing {

	private File file;
	private char delimiter;

	// csv reader 2.0
	private CsvReader reader = null;
	
	// sql methods
	private Optimise sql = new Optimise();
	
	/**
	 * constructor
	 */
	public CSVProcessing() {
	}
	
	protected void setData(char delimiter, DestinationData destinationData, MatchFieldData[] matchFields) {
		this.delimiter = delimiter;
		
		try {
			sql.setDestinationData(destinationData);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR: CSVProcessing: forogot destinationData is null");
			System.exit(1);
		}
		sql.setMatchFields(matchFields);
		
	}
	
	public void dropTableOff() {
		sql.dropTableOff();
	}
	
	/** 
	 * start extracting the data
	 */
	protected void parseFile(int indexFile, File file) {
		this.file = file;
		
		// open a sql connection
		sql.openConnection();
		
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
		sql.runOptimise();
		
		// delete empty columns (if set on)
		sql.deleteEmptyColumns();
		
		// done with connection
		sql.closeConnection();
	}
	
	private void readFile() {
		try {
			reader = new CsvReader(file.toString(), delimiter);
		} catch (FileNotFoundException e) {
			System.err.println("CSV Reader, Could not open CSV Reader");
			e.printStackTrace();
		}
	}
	
	/**
	 * get columns from file, and create them in database if need be
	 * 
	 * @throws Exception
	 */
	private void getColumns() throws Exception {
		ColumnData[] columns = null;	
		try {
			reader.readHeaders();
			columns = makeColumnlist(reader.getHeaders());
			System.out.println("column count: " + reader.getHeaderCount());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// insert columns if need be
		if (columns != null) {
			sql.createColumns(columns);
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
			sql.addData(indexFile, index, values);
			
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
	
	private ColumnData[] makeColumnlist(String[] columns) {
		ColumnData[] cols = new ColumnData[columns.length];
		for(int i=0; i < columns.length; i++) {
			cols[i] = new ColumnData();
			cols[i].column = columns[i];
		}
		return cols;
	}
	
	
	
	
}
