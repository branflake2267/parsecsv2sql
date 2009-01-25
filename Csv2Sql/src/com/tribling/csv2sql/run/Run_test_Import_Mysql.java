package com.tribling.csv2sql.run;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_test_Import_Mysql {
	
	/**
	 * start it
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		run();
	}
	
	public static void run() {
		
		char commaDelimiter = ',';
		char tabDelimiter = '	';
		
		String file = "/home/branflake2267/downloads/parse/test";
		SourceData sourceData = new SourceData();
		sourceData.delimiter = commaDelimiter;
		sourceData.file = new File(file);
		
		
		DestinationData dd = new DestinationData();
		dd.dropTable = true;
		dd.checkForExistingRecordsAndUpdate = true;
		dd.optimise = true;
		dd.createIndexs = true;
		dd.deleteEmptyColumns = true;
		dd.optimiseRecordsToExamine = 1000; // 0 will do all
		dd.optimiseTextOnly = true;
		dd.databaseType = "MySql";
		dd.host = "192.168.10.91";
		dd.database = "test";
		dd.username = "test";
		dd.password = "test";
		dd.port = "3306";
		dd.table = "tmp_test"; 
		
		
		MatchFieldData[] matchFields = new MatchFieldData[1];
		matchFields[0] = new MatchFieldData();
		matchFields[0].sourceField = "ID";
		matchFields[0].destinationField = "UID";

		MatchFieldData[] idents = new MatchFieldData[1];
		idents[0] = new MatchFieldData();
		idents[0].sourceField = "ID";
		idents[0].destinationField = "UID";
		dd.identityColumns = idents;

		FileProcessing process = new FileProcessing();
		process.setData(sourceData, dd, matchFields);
		
		
	}
}
