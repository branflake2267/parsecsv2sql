package com.tribling.csv2sql.data;

/**
 * Destination values - controls insert/update methods
 * 
 * @author branflake2267
 *
 */
public class DestinationData {

	// drop table before insert 
	// (if looping files in a directory, it will drop table in the beginning)
	public boolean dropTable = false;
	
	// auto optimise at the end of inserting/updating records
	// this will skip optimising table and can do this separately if needed.
	public boolean optimise = false;
	
	// how many records to examine for optimisation of the column data
	// records are examine randomly over this many records
	public int optimiseRecordsToExamine = 1000;
	
	// optimise lengths into varchar and text only
	// this will have to be done first every time, so that
	// one can go from varchar->date, varchar->int (instead of text>date=will not work)
	public boolean optimiseTextOnly = true;
	
	// delete empty columns after parsing a table
	public boolean deleteEmptyColumns = false;
	
	
	// [MySql|MsSql] - MySql or Microsoft SQL databases methods available
	public String databaseType = "MySql";
	
	// datbase settings
	public String host;
	public String database;
	public String username;
	public String password;
	public String port;
	
	// table for records to exist
	public String table;

	// this only appears in MsSql that I know of. select * from [catalog].[tableSchema].[table]
	public String tableSchema;

	// identiy columns to match update instead of insert?
	public MatchFieldData[] identityColumns;
}
