package com.tribling.csv2sql.data;

/**
 * Destination values - controls insert/update methods
 * 
 * @author branflake2267
 *
 */
public class DestinationData {

  // drop table if exists before insert 
  // (if looping files in a directory, it will drop table in the beginning)
  public boolean dropTable = false;

  // auto optimise at the end of inserting/updating records
  // this will skip optimising table and can do this separately if needed.
  public boolean optimise = false;

  // how many records to examine for optimisation of the column data
  // records are examine randomly over this many records
  public int optimise_RecordsToExamine = 1000;

  // when Examining the data, examine the recores in the column it randomly
  public boolean optimise_skipRandomExamine = false;

  // when Examing the data, ignore null values, this will slow things down in huge record sets
  public boolean optimise_ignoreNullFieldsWhenExamining = true;

  // optimise lengths into varchar and text only
  // this will have to be done first every time, so that
  // one can go from varchar->date, varchar->int (instead of text>date=will not work)
  public boolean optimise_TextOnly = true;

  // use this if you need to repeat optimisation and want to skip all but TEXT columns
  public boolean optimise_TextOnlyColumnTypes = false;
  

  // this will delete indexes, then optimise
  // a column can't have a index on it if it needs altering
  public boolean skipDeletingIndexingBeforeOptimise = false;

  // delete empty columns after parsing a table
  public boolean deleteEmptyColumns = false;

  // if a record exists with identity columns update
  // Identity Columns data needs to be exact (its explicit!)
  public boolean checkForExistingRecordsAndUpdate = false;

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

  // if identity columns are given, and this is true, 
  // create indexes of the identity columns
  // Also will need Identity Columns listed
  public boolean createIndexs = true;


  /**
   * make database type an integer
   * @return
   */
  public int getDbType() {
    databaseType = databaseType.toLowerCase();
    int type = 0;
    if (this.databaseType.equals("mysql")) {
      type = 1;
    } else if (databaseType.equals("mssql")) {
      type = 2;
    } else {
      System.err.println("ERROR: No DatabaseTye: [MySql|MsSql]");
      System.exit(1);
    }
    return type;
  }
}
