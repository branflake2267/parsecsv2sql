package com.tribling.csv2sql.run;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.OptimiseTable;
import com.tribling.csv2sql.SQLProcessing;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Import_SurveyData_Long_MySql {

	private static DestinationData destinationData = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// import records
		run();

		// delete bogus
		deleteBogusData();
		
		// optimise - simple
		destinationData.optimise = true;
		runOptimise();
		
	}
	
	public static void run() {
		char delimiter = '	';
		
		String file = "/home/branflake2267/downloads/long";
		SourceData sourceData = new SourceData();
		sourceData.delimiter = delimiter;
		sourceData.file = new File(file);
		
		
		destinationData = new DestinationData();
		destinationData.dropTable = true;
		destinationData.optimise = false;
		destinationData.optimiseTextOnly = true;
		destinationData.deleteEmptyColumns = true;
		destinationData.checkForExistingRecordsAndUpdate = false; // not unique
		destinationData.optimiseRecordsToExamine = 2500;
		destinationData.databaseType = "MySql";
		destinationData.host = "192.168.10.91";
		destinationData.database = "survey";
		destinationData.username = "Branflake2267";
		destinationData.password = "GoneVertical**89";
		destinationData.port = "3306";
		destinationData.tableSchema = ""; // not in mysql
		destinationData.table = "wccci_tmp_surveydata_long"; // must ecapsulate ms sql [schema].[db].[table]
		
		
		// NOTE: need more to make unique
		// has to be the source identies
		MatchFieldData[] idents = new MatchFieldData[2];
		idents[0] = new MatchFieldData();
		idents[1] = new MatchFieldData();
		
		idents[0].sourceField = "Resp#";
		idents[0].desinationField = "Resp_Num";
		
		idents[1].sourceField = "QType";
		idents[1].desinationField = "SurveyType";
		
		
		destinationData.identityColumns = idents;
		
		int matchCount = 17;
		MatchFieldData[] matchFields = new MatchFieldData[matchCount];
		for (int i=0; i < matchCount; i++) {
			matchFields[i] = new MatchFieldData();
		}
		
		matchFields[0].sourceField = "Orig Empl Id";
		matchFields[0].desinationField = "EmployeeID";
		
		matchFields[1].sourceField = "R.1";
		matchFields[1].desinationField = "NPS";
		
		matchFields[2].sourceField = "Tech Id";
		matchFields[2].desinationField = "TechID";
		
		matchFields[3].sourceField = "Trouble Tkt";
		matchFields[3].desinationField = "TTN";
		
		matchFields[4].sourceField = "QType";
		matchFields[4].desinationField = "SurveyType";
		
		matchFields[5].sourceField = "Empl ID";
		matchFields[5].desinationField = "EmployeeID";
		
		matchFields[6].sourceField = "	";
		matchFields[6].desinationField = "EmployeeID";
		
		matchFields[7].sourceField = "Employee Id";
		matchFields[7].desinationField = "EmployeeID";
		
		matchFields[8].sourceField = "EmpID";	
		matchFields[8].desinationField = "EmployeeID";
		
		matchFields[9].sourceField = "OrigEmpl";
		matchFields[9].desinationField = "EmployeeID";
			
		matchFields[10].sourceField = "Ts Cps Trbl";
		matchFields[10].desinationField = "TTN";
		
		matchFields[11].sourceField = "TasCopsTrblTckt";
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
	
		/* also has mast ord# in the row
		matchFields[17].sourceField = "Prov Ord#";
		matchFields[17].desinationField = "TTN2";
		*/
		
		
		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
	}
	
	
	private static void deleteBogusData() {

		// delete - dosn't have a survey type
		// NOTICE:  NOT FOR USE OR DISCLOSURE OUTSIDE THE VERIZON COMPANIES
		// EXCEPT UNDER WRITTEN AGREEMENT
		
		String query = "";
		if (destinationData.databaseType.equals("MySql") == true) {
			query = "DELETE FROM survey.wccci_tmp_surveydata_long WHERE (surveytype= '')";
		} else if (destinationData.databaseType.equals("MySql") == true){
			query = "DELETE FROM dbo.wccci_tmp_surveydata_long WHERE (surveytype= '')";
		}
		
		SQLProcessing sql = new SQLProcessing();
		try {
			sql.setDestinationData(destinationData);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
