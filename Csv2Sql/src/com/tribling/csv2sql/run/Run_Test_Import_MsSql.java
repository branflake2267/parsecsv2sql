package com.tribling.csv2sql.run;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.OptimiseTable;
import com.tribling.csv2sql.SQLProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Test_Import_MsSql {


	private static DestinationData destinationData = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// import records
		run();

		// optimise - simple
		destinationData.optimise = true;
		runOptimise();
	
		System.out.println("The End");
	}
	
	public static void run() {
		char delimiter = '	';
		
		String file = "C:\\Archive\\parse\\filesInthisDir";
		SourceData sourceData = new SourceData();
		sourceData.delimiter = delimiter;
		sourceData.file = new File(file);
		
		
		destinationData = new DestinationData();
		destinationData.dropTable = true;
		destinationData.optimise = false;
		
		// this will only do varchar optimisation, but if false, will do ints and dates too
		destinationData.optimiseTextOnly = true;
		destinationData.deleteEmptyColumns = true;
		destinationData.checkForExistingRecordsAndUpdate = false; 
		destinationData.optimiseRecordsToExamine = 2500;
		
		destinationData.databaseType = "MsSql";
		destinationData.host = "";
		destinationData.database = "";
		destinationData.username = "";
		destinationData.password = "";
		destinationData.port = "1433";
		destinationData.tableSchema = "dbo";
		destinationData.table = "tmp_table";

		
		
		// use these unique ids to compare to existing data
		MatchFieldData[] idents = new MatchFieldData[2];
		idents[0] = new MatchFieldData();
		idents[1] = new MatchFieldData();
		
		idents[0].sourceField = "Orig Id";
		idents[0].desinationField = "EmployeeID";
		
		idents[1].sourceField = "id";
		idents[1].desinationField = "ID";
		
		// setup idents to work with if I want to update
		destinationData.identityColumns = idents;
		
		// init object for data storage
		int matchCount = 17;
		MatchFieldData[] matchFields = new MatchFieldData[matchCount];
		for (int i=0; i < matchCount; i++) {
			matchFields[i] = new MatchFieldData();
		}
		
		matchFields[0].sourceField = "Orig Id";
		matchFields[0].desinationField = "EmployeeID";
		
		matchFields[1].sourceField = "q1";
		matchFields[1].desinationField = "q1.1";
		
		matchFields[2].sourceField = "Tech Id";
		matchFields[2].desinationField = "TechID";
		
		matchFields[3].sourceField = "Trouble Tkt";
		matchFields[3].desinationField = "TTN";
		
		matchFields[4].sourceField = "QType";
		matchFields[4].desinationField = "SurveyType";
		
		matchFields[5].sourceField = "Empl ID";
		matchFields[5].desinationField = "EmployeeID";
		
		matchFields[6].sourceField = "Orig Empl Id";// TODO 
		matchFields[6].desinationField = "EmployeeID";
		
		matchFields[7].sourceField = "Employee Id";
		matchFields[7].desinationField = "EmployeeID";
		
		matchFields[8].sourceField = "EmpID";	
		matchFields[8].desinationField = "EmployeeID";
		
		matchFields[9].sourceField = "OrigEmpl";
		matchFields[9].desinationField = "EmployeeID";
			
		matchFields[10].sourceField = "Ts Cps Trbl";
		matchFields[10].desinationField = "TTN";
		
		matchFields[11].sourceField = "TasCoTrblTckt";
		matchFields[11].desinationField = "TTN";

		matchFields[12].sourceField = "WizOrder";
		matchFields[12].desinationField = "TTN";
		
		matchFields[13].sourceField = "Sub Id";
		matchFields[13].desinationField = "EmployeeID";
		
		matchFields[14].sourceField = "Service Ord";
		matchFields[14].desinationField = "TTN";
		
		matchFields[15].sourceField = "Orig Emp ID";
		matchFields[15].desinationField = "EmployeeID";

		matchFields[16].sourceField = "Mast Ord#";
		matchFields[16].desinationField = "TTN2";

		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
	}
	
	/**
	 * optimise the table in the end
	 */
	private static void runOptimise() {
		OptimiseTable o = new OptimiseTable();
		try {
			o.setDestinationData(destinationData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		o.runOptimise();
	}

}
