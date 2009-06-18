package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.MatchFieldData;

/**
 * transfer data from one table to another
 * 
 * @author design
 *
 */
public class Transfer extends SQLProcessing {

  // what kind of transfer process is going on
  public static final int MODE_TRANSFER_ALL = 1;
  public static final int MODE_TRANSFER_ONLY = 2;
  private int mode = 0;
  
  // source database settings
  private DatabaseData database_src = null;
  
  // destination database settings
  private DatabaseData database_dest = null;
  
  // table being copied
  private String table = null;
  
  // table identity fields established so not to create duplicates
  private MatchFieldData[] identFields = null;

  // table mapped fields from src to data
  private MatchFieldData[] mappedFields = null;

  // column data structure which will be the destinations 
  private ColumnData[] columnData_src = null;
  private ColumnData[] columnData_des = null;
  
  /**
   * Transfer data object setup 
   * 
   * @param database_src
   * @param database_dest
   * @param identFields
   * @param mappedFields
   */
  public Transfer(DatabaseData database_src, DatabaseData database_dest) {
    this.database_src = database_src;
    this.database_dest = database_dest;
  }
  
  /**
   * start the data transfer
   * 
   * @param table
   * @param identFields_src
   * @param mappedFields
   */
  public void setData(String table, MatchFieldData[] mappedFields) {
    this.table = table;
    this.mappedFields = mappedFields;
  }
  
  public void transferAllFields() {
    this.mode = MODE_TRANSFER_ALL;
    start();
  }
  
  public void transferOnlyMappedFields() {
    this.mode = MODE_TRANSFER_ONLY;
    start();
  }
  
  /**
   * prep the objects needed
   */
  private void start() {
    
    database_src.openConnection();
    database_dest.openConnection();
    
    // set up the object structure to transfer the data through
    if (mode == MODE_TRANSFER_ONLY) {
      setColumnDataStructure_Mapped_Only();
    } else if (mode == MODE_TRANSFER_ALL) {
      // TODO
    }
    
    process();
    
    database_src.closeConnection();
    database_dest.closeConnection();
    
  }
  
  private void setColumnDataStructure_Mapped_Only() {
    
    columnData_src = new ColumnData[mappedFields.length];
    columnData_des = new ColumnData[mappedFields.length];
    for (int i = 0; i < mappedFields.length; i++) {
      columnData_src[i] = new ColumnData();
      columnData_des[i] = new ColumnData();
      
      columnData_src[i].column = mappedFields[i].sourceField;
      //columnData_src[i].setIsPrimaryKey(mappedFields[i].isIdentity);
      
      columnData_des[i].column = mappedFields[i].destinationField;
      //columnData_des[i].setIsPrimaryKey(mappedFields[i].isIdentity);
    }

  }
  
  private void process() {

    String sql = "";
    sql = "SELECT * FROM `" + table + "` ";
    
    try {
      Connection conn = database_src.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        for (int i=0; i < columnData_src.length; i++) {
          String value = result.getString(columnData_src[i].getColumnName());
          columnData_des[i].setValue(value);
        }
        
        save();
      }
      result.close();
      select.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }

  private void save() {
    
    int id = doIdentsExistAlready(database_dest);
    
    if (id > 0) { // update
      // TODO generate fields
    } else { // insert
      // TODO generate fields
    }

  }
  
  private int doIdentsExistAlready(DatabaseData databaseData) {
    setDatabaseData(databaseData);
    openConnection();
    
    String primaryKey = getPrimaryKeyId(databaseData);
    
    String sql = "Select `" + primaryKey + "` FROM `" + table + "` WHERE "  + getSqlIdent();
    
    int id = getQueryInt(sql);
    
    closeConnection();
    
    return id;
  }
  
  private String getPrimaryKeyId(DatabaseData databaseData) {
    
    setDatabaseData(databaseData);
    openConnection();
    
    ColumnData columnData = getPrimaryKey();
    
    String primaryKey = columnData.getColumnName();
    
    closeConnection();
    
    return primaryKey;
  }

  private String getSqlIdent() {
    
    String sql = "";
    for (int i=0; i < columnData_des.length; i++) {
      if (columnData_des[i].getIsPrimaryKey() == true) {
        
        sql = "(`" + columnData_des[i].getColumnName() + "`='" + SQLProcessing.escapeForSql(columnData_des[i].getValue()) + "')";
        
        if (i < columnData_des.length -1) {
          sql = " AND ";
        }
        
      }
    }
    
    return sql;
  }
  

  
  
}
