package com.tribling.csv2sql.data;

public class DestinationData {

	// drop table before insert
	public boolean dropTable = false;
	
	// skipping optimising table (you can do this sparately if needed.)
	public boolean optimise = false;
	
	// how many records to examine
	public int optimiseRecordsToExamine = 1000;
	
	
	// [MySql|MsSql]
	public String databaseType = "MySql";
	
	// datbase settings
	public String host;
	public String database;
	public String username;
	public String password;
	public String port;
	
	public String table;
	public MatchFieldData[] identityColumns;

	public String tableSchema;

	
	
}
