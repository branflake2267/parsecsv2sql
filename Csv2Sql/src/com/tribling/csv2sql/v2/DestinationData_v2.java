package com.tribling.csv2sql.v2;

import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.FieldData;

/**
 * Destination values - controls insert/update methods
 * 
 * @author branflake2267
 *
 */
public class DestinationData_v2 {

  // database connection
  public DatabaseData databaseData = null;

  // match these fields and change them 
  public FieldData[] changeColumn = null;
  
  // use these columns for identity, when adding data
  //   that has similarity matching 
  public FieldData[] identityColumns;
  
  // db table
  public String table;
  
  
  
  // default primary key name on import
  public String primaryKeyName = "Auto_ImportId";
  
  // drop the table being added to in the beginning
  public boolean dropTable = false;

  // auto optimise at the end of adding data
  public boolean optimise = false;

  // how many records to examine for optimisation of the column data
  // records are examine randomly over this many records
  public int optimise_RecordsToExamine = 1000;

  // when Examining the data, examine the recores in the column it randomly
  public boolean optimise_skipRandomExamine = false;

  // when Examing the data, ignore null values, this will slow things down in huge record sets
  public boolean optimise_ignoreNullFieldsWhenExamining = true;

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

  // if identity columns are given, and this is true, 
  // create indexes of the identity columns
  // Also will need Identity Columns listed
  public boolean createIndexs = true;

  // when no field names exist in the first row, make this true
  public boolean firstRowHasNoFieldNames = false;

  // save the source file into the records srcFile= /home/branflake2267/home.txt
  public boolean setSrcFileIntoColumn = false;
  
  // after processing the file, move it to the done folder
  public boolean moveFileToDone = false;
  
  // stop at column, don't process any columns over this count. 0 process all
  public int stopAtColumnCount = 0;
  
  /**
   * optimize table settings basics
   * 
   * @param databaseData
   * @param table
   */
  public void setData(DatabaseData databaseData, String table) {
    this.databaseData = databaseData;
    this.table = table;
  }
  
  /**
   * set the necessities
   * 
   * @param databaseData
   * @param changeColumns
   * @param identities
   * @param table
   */
  public void setData(DatabaseData databaseData, FieldData[] changeColumns, FieldData[] identities, String table) {
    this.databaseData = databaseData;
    this.changeColumn = changeColumns;
    this.identityColumns = identities;
    this.table = table;
  }
}
