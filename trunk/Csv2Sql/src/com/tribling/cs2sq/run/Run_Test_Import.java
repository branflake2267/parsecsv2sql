package com.tribling.cs2sq.run;

import java.io.File;
import java.util.Arrays;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Test_Import {
	
	public static void main(String[] args) {
		run();
	}
	
	public static void run() {
		
		char commaDelimiter = ',';
		char tabDelimiter = '	';
		
		String file = "C:\\Archive\\test.txt";
		SourceData sourceData = new SourceData();
		sourceData.delimiter = tabDelimiter;
		sourceData.file = new File(file);
		
		
		DestinationData destinationData = new DestinationData();
		destinationData.dropTable = true;
		destinationData.databaseType = "l";
		destinationData.host = "";
		destinationData.database = "";
		destinationData.username = "";
		destinationData.password = "";
		destinationData.port = "1433";
		destinationData.tableSchema = "dbo";
		destinationData.table = "tmp_test"; // must ecapsulate ms sql [schema].[db].[table]
		
		/*
		* TODO - add identity system
		MatchFieldData[] identities = new MatchFieldData[3];
		identities[0] = new MatchFieldData();
		identities[0].sourceField = "";
		identities[0].desinationField = "";
		identities[1] = new MatchFieldData();
		identities[1].sourceField = "";
		identities[1].desinationField = "";
		identities[2] = new MatchFieldData();
		identities[2].sourceField = "";
		identities[2].desinationField = "";
		destinationData.identityColumns = identities;
		*/
		
		MatchFieldData[] matchFields = new MatchFieldData[4];
		matchFields[0] = new MatchFieldData();
		matchFields[0].sourceField = "Orig Empl Id";
		matchFields[0].desinationField = "EmployeeID";
		matchFields[1] = new MatchFieldData();
		matchFields[1].sourceField = "R1";
		matchFields[1].desinationField = "NPS";
		matchFields[2] = new MatchFieldData();
		matchFields[2].sourceField = "Tech Id";
		matchFields[2].desinationField = "TechID";
		matchFields[3] = new MatchFieldData();
		matchFields[3].sourceField = "Trouble Tkt";
		matchFields[3].desinationField = "TTN";
		

		
		
		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
		
		
	}
}
