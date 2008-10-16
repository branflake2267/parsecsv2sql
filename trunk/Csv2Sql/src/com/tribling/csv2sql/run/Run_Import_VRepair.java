package com.tribling.csv2sql.run;

import java.io.File;

import com.tribling.csv2sql.FileProcessing;
import com.tribling.csv2sql.OptimiseTable;
import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

public class Run_Import_VRepair {

	public static DestinationData destinationData = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// import records
		run();
		
		// optimise
		runOptimise();
		
	}

	public static void run() {
		
		char delimiter = ',';
	
		String file = "C:\\Archive\\parse\\vrepair2\\tickets.csv";  //C:\\Archive\\parse\\vrepair
		//String file = "C:\\Archive\\parse\\vrepair";
		SourceData sourceData = new SourceData();
		sourceData.delimiter = delimiter;
		sourceData.file = new File(file);
		
		
		destinationData = new DestinationData();
		destinationData.dropTable = false;
		destinationData.optimise = false;
		destinationData.databaseType = "MsSql";
		destinationData.host = "144.8.13.225";
		destinationData.database = "WAFSC";
		destinationData.username = "WAFSC_user";
		destinationData.password = "April98";
		destinationData.port = "1433";
		destinationData.tableSchema = "dbo";
		destinationData.table = "wccci_tmp_tickets_vrepair"; // must ecapsulate ms sql [schema].[db].[table]
		
		// CLOSED_DT,SOURCE_SYS_TROUBLE_ID,CLOSED_BY_EMP_ID
		
		MatchFieldData[] matchFields = new MatchFieldData[5];
		matchFields[0] = new MatchFieldData();
		matchFields[0].sourceField = "L1";
		matchFields[0].desinationField = "L1";
		matchFields[1] = new MatchFieldData();
		matchFields[1].sourceField = "CLOSED_DT";
		matchFields[1].desinationField = "ClosedDate";
		matchFields[2] = new MatchFieldData();
		matchFields[2].sourceField = "SOURCE_SYS_TROUBLE_ID";
		matchFields[2].desinationField = "TTN";
		matchFields[3] = new MatchFieldData();
		matchFields[3].sourceField = "CLOSED_BY_EMP_ID";
		matchFields[3].desinationField = "VzId";
		matchFields[4] = new MatchFieldData();
		matchFields[4].sourceField = "Expr1";
		matchFields[4].desinationField = "State";
		
		FileProcessing process = new FileProcessing();
		process.setData(sourceData, destinationData, matchFields);
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
