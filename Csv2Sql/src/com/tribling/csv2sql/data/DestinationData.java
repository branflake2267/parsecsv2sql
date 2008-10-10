package com.tribling.csv2sql.data;

public class DestinationData {

	// drop table before insert
	public boolean dropTable = false;
		
	// [MySql|MsSql]
	public String databaseType = "MySql";
	
	// datbase settings
	public String host;
	public String database;
	public String username;
	public String password;
	public String port;
	
	public MatchFieldData[] identityColumns;
	
}
