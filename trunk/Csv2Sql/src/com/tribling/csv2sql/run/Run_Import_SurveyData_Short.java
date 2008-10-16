package com.tribling.csv2sql.run;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.OptimiseTable;
import com.tribling.csv2sql.SQLProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Import_SurveyData_Short {

	private static DestinationData destinationData = null;
	
	public static void main(String[] args) {
		
		//import records
		run();
		
		// optimise simple
		runOptimise();
		
		// delete bogus data
		deleteBogusData();
	}

	public static void run() {
		char delimiter = ',';

		String file = "C:\\Archive\\parse\\short\\short.csv"; 
		SourceData sourceData = new SourceData();
		sourceData.delimiter = delimiter;
		sourceData.file = new File(file);

		// identity 
		//Unique respondent identifier
		
		destinationData = new DestinationData();
		destinationData.dropTable = true;
		destinationData.optimise = false;
		destinationData.optimiseRecordsToExamine = 2000;
		destinationData.databaseType = "MsSql";
		destinationData.host = "144.8.13.225";
		destinationData.database = "WAFSC";
		destinationData.username = "WAFSC_user";
		destinationData.password = "April98";
		destinationData.port = "1433";
		destinationData.tableSchema = "dbo";
		destinationData.table = "wccci_tmp_surveydata_short";

		int matchCount = 14;
		MatchFieldData[] matchFields = new MatchFieldData[matchCount];
		for (int i = 0; i < matchCount; i++) {
			matchFields[i] = new MatchFieldData();
		}

		matchFields[0].sourceField = "Interview Reporting Period";
		matchFields[0].desinationField = "Period";

		// short
		matchFields[1].sourceField = "3.0 How likely is it that you would recommend Verizon to a friend or colleague?";
		matchFields[1].desinationField = "NPS";

		matchFields[2].sourceField = "Tech Id";
		matchFields[2].desinationField = "TechID";

		matchFields[3].sourceField = "Trouble Tkt";
		matchFields[3].desinationField = "TTN";

		// short
		matchFields[4].sourceField = "Questionnaire Type";
		matchFields[4].desinationField = "SurveyType";

		matchFields[5].sourceField = "Empl ID";
		matchFields[5].desinationField = "EmployeeID";

		matchFields[6].sourceField = "Orig Empl Id";
		matchFields[6].desinationField = "EmployeeID";

		matchFields[7].sourceField = "Employee Id";
		matchFields[7].desinationField = "EmployeeID";

		matchFields[8].sourceField = "EmpID";
		matchFields[8].desinationField = "EmployeeID";

		matchFields[9].sourceField = "OrigEmpl";
		matchFields[9].desinationField = "EmployeeID";

		matchFields[10].sourceField = "Ts Cps Trbl";
		matchFields[10].desinationField = "TTN";

		// short
		matchFields[11].sourceField = "TAS-COPS Trouble Ticket #";
		matchFields[11].desinationField = "TTN";

		// short
		matchFields[12].sourceField = "Originating Employee ID";
		matchFields[12].desinationField = "EmployeeID";

		// short
		matchFields[13].sourceField = "Technician ID";
		matchFields[13].desinationField = "TechID";

		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
	}

	private static void deleteBogusData() {

		// delete - dosn't have a survey type
		// NOTICE:  NOT FOR USE OR DISCLOSURE OUTSIDE THE VERIZON COMPANIES
		// EXCEPT UNDER WRITTEN AGREEMENT
		
		System.out.println("Deleting bogus data");
		
		String query = "";
		if (destinationData.databaseType.equals("MySql") == true) {
			query = "DELETE FROM dbo.wccci_tmp_surveydata_long WHERE (surveytype= '')";
		} else if (destinationData.databaseType.equals("MySql") == true){
			query = "DELETE FROM dbo.wccci_tmp_surveydata_long WHERE (surveytype= '')";
		}
		
		SQLProcessing sql = new SQLProcessing();
		sql.setUpdateQuery(query);
		
	}
	
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
