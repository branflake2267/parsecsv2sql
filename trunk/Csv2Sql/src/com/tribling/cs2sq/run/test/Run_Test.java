package com.tribling.cs2sq.run.test;

import java.beans.DesignMode;
import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Test {
	
	public Run_Test() {
		
		String file = "";
		char commaDelimiter = ',';
		char tabDelimiter = '	';
		SourceData sourceData = new SourceData();
		sourceData.delimiter = tabDelimiter;
		sourceData.file = new File(file);
		
		
		DestinationData destinationData = new DestinationData();
		destinationData.dropTable = false;
		destinationData.optimise = true;
		destinationData.deleteEmptyColumns = true;
		destinationData.databaseType = "MySql";
		destinationData.host = "";
		destinationData.database = "";
		destinationData.username = "";
		destinationData.password = "";
		destinationData.port = "";
		
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
		
		
		MatchFieldData[] matchFields = new MatchFieldData[2];
		matchFields[0] = new MatchFieldData();
		matchFields[0].sourceField = "Tech ID";
		matchFields[0].desinationField = "TechnicianID";
		matchFields[1] = new MatchFieldData();
		matchFields[1].sourceField = "OrgEmpID";
		matchFields[1].desinationField = "EmployeeID";
		matchFields[2] = new MatchFieldData();
		matchFields[2].sourceField = "Org Emp ID";
		matchFields[2].desinationField = "EmployeeID";
		
		
		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
		
		
	}
}
