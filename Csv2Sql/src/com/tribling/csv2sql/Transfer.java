package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.FieldData;

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
  private DatabaseData database_des = null;
  
  // transfering from table to table
  private String tableFrom = null;
  private String tableTo = null;
  
  // table identity fields established so not to create duplicates
  private FieldData[] identFields = null;

  // table mapped fields from src to data
  private FieldData[] mappedFields = null;

  // table mapped fields for one to many
  private FieldData[] oneToMany = null;
  
  // column data structure which will be the destinations 
  private ColumnData[] columnData_src = null;
  private ColumnData[] columnData_des = null;
  
  private ColumnData[] columnData_src_oneToMany = null;
  private ColumnData[] columnData_des_oneToMany = null;
  
  // pass around during processing of one to many items
  private ColumnData oneToManyTablePrimaryKey = null;
  
  // one to many relationship like userId='1'
  private String oneToMany_RelationshipSql = null;
  
  // hard code values in on a one to many records
  private ArrayList<HashMap<String,String>> hardOneToMany = new ArrayList<HashMap<String,String>>();
   
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
    this.database_des = database_dest;
  }

  /**
   * set up tables from to
   * 
   * @param fromTable
   * @param toTable
   */
  public void transferAllFields(String fromTable, String toTable) {
    this.mode = MODE_TRANSFER_ALL;
    this.tableFrom = fromTable;
    this.tableTo = toTable;
    
    // TODO - get all fields here into a mappedFields object
    
    start();
  }
  
  /**
   * set up tables from to and mapped fields to transfer
   * 
   * @param fromTable
   * @param toTable
   * @param mappedFields
   */
  public void transferOnlyMappedFields(String fromTable, String toTable, FieldData[] mappedFields, FieldData[] oneToMany) {
    this.mode = MODE_TRANSFER_ONLY;
    this.tableFrom = fromTable;
    this.tableTo = toTable;
    this.mappedFields = mappedFields;
    this.oneToMany  = oneToMany;
    start();
  }
  
  /**
   * prep the objects needed
   */
  private void start() {
    
    database_src.openConnection();
    database_des.openConnection();

    setColumnData();

    processSrc();
    
    database_src.closeConnection();
    database_des.closeConnection();
    
  }
  
  private void setColumnData() {
    
    columnData_src = new ColumnData[mappedFields.length];
    columnData_des = new ColumnData[mappedFields.length];
    for (int i = 0; i < mappedFields.length; i++) {
      columnData_src[i] = new ColumnData();
      columnData_des[i] = new ColumnData();
      
      columnData_src[i].setColumnName(mappedFields[i].sourceField);
      columnData_src[i].setIsPrimaryKey(mappedFields[i].isPrimaryKey);
      columnData_src[i].setOverwriteWhenBlank(mappedFields[i].onlyOverwriteBlank);
      columnData_src[i].setOverwriteWhenZero(mappedFields[i].onlyOverwriteZero);
      columnData_src[i].setRegex(mappedFields[i].regexSourceField);
      
      columnData_des[i].setColumnName(mappedFields[i].destinationField);
      columnData_des[i].setIsPrimaryKey(mappedFields[i].isPrimaryKey);
      columnData_des[i].setOverwriteWhenBlank(mappedFields[i].onlyOverwriteBlank);
    }

    columnData_src_oneToMany = new ColumnData[oneToMany.length];
    columnData_des_oneToMany = new ColumnData[oneToMany.length];
    for (int i=0; i < oneToMany.length; i++) {
      columnData_src_oneToMany[i] = new ColumnData();
      columnData_des_oneToMany[i] = new ColumnData();
      
      columnData_src_oneToMany[i].setColumnName(oneToMany[i].sourceField);
      columnData_src_oneToMany[i].setOverwriteWhenBlank(oneToMany[i].onlyOverwriteBlank);
      columnData_src_oneToMany[i].setRegex(oneToMany[i].regexSourceField);
      
      columnData_des_oneToMany[i].setColumnName(oneToMany[i].destinationField);
      columnData_des_oneToMany[i].setOverwriteWhenBlank(oneToMany[i].onlyOverwriteBlank);
      columnData_des_oneToMany[i].setRegex(oneToMany[i].regexSourceField);
      columnData_des_oneToMany[i].setTable(oneToMany[i].differentDestinationTable);
      hardOneToMany.add(oneToMany[i].hardOneToMany);
    }
    
  }
  
  private void processSrc() {

    String sql = "";
    sql = "SELECT * FROM " + tableFrom + " ";
    
    try {
      Connection conn = database_src.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        for (int i=0; i < columnData_src.length; i++) {
          String value = result.getString(columnData_src[i].getColumnName());
          columnData_src[i].setValue(value);
        }
        
        for (int i=0; i < columnData_src_oneToMany.length; i++) {
          String value = result.getString(columnData_src_oneToMany[i].getColumnName());
          columnData_src_oneToMany[i].setValue(value);
        }
        
        process();
      }
      result.close();
      select.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }
  
  /**
   * compare for overwrite checking
   */
  private void process() {
    
    // TODO - what if no values exist on the other end
    getDestinationValuesToCompareWith(columnData_src, columnData_des);
    
    getDestinationValuesToCompareWith_OneToMany(columnData_src_oneToMany, columnData_des_oneToMany);
    
    merge();
    
    merge_OneToMany();
    
    save();
    
    saveOneToMany();
  }
  
  private void saveOneToMany() {
    
   // does the value already exist?
   for (int i=0; i < columnData_des_oneToMany.length; i++) {
     int onetoId = getOneToManyId(columnData_des_oneToMany[i], hardOneToMany.get(i));
     
     saveOneToMany(onetoId, columnData_des_oneToMany[i], hardOneToMany.get(i));
   }
    
  }
  
  private void saveOneToMany(int onetoId, ColumnData columnData, HashMap<String,String> hardOneToMany) {
    
    String hardFields = getFields_OneToMany_Hard(hardOneToMany, 1);
    
    String fields = getFields_OneToMany();
    if (hardFields.length() > 0) {
      fields += ", " + hardFields;
    }
    
    String sql = "";
    if (onetoId > 0) { // update?
      fields += ",DateUpdated=NOW()";
      String where = "(`" + oneToManyTablePrimaryKey.getColumnName() + "`='" + onetoId + "')";
      sql = "UPDATE " + columnData.getTable() + " SET " + fields + " WHERE " + where;
    } else { // insert
      fields += ",DateCreated=NOW()";
      sql = "INSERT INTO " + columnData.getTable() + " SET " + fields;
    }
    
    updateSql_v2(database_des, sql);
  }

  private int getOneToManyId(ColumnData columnData, HashMap<String,String> hardOneToMany) {
    
    // primary key
    oneToManyTablePrimaryKey = getPrimaryKey_MySql_v2(database_des, columnData.getTable());
    
    String where = getOneToManySqlWhere(columnData, hardOneToMany);

    String sql = "SELECT " + oneToManyTablePrimaryKey.getColumnName() + " FROM " + columnData.getTable() + " WHERE " + where;
    
    int id = getQueryInt_v2(database_des, sql);
    
    return id;
  }
  
  private String getOneToManySqlWhere(ColumnData columnData, HashMap<String,String> hardOneToMany) {

    String whereHard = "";
    if (hardOneToMany.isEmpty() == false) {
      whereHard = " AND " + getFields_OneToMany_Hard(hardOneToMany, 2);
    }
    
    String where = getWhere() + " AND " +
        "(`" + columnData.getColumnName()+"`='" + columnData.getValue() + "') " + whereHard;
 
    return where;
  }

  private void save() {
    
    int id = doIdentsExistAlready(database_des);
    
    String fields = getFields();
    
    String where = getWhere();
    
    String sql = "";
    if (id > 0) { // update
      fields += ",DateUpdated=NOW()";
      sql = "UPDATE " + tableTo + " SET " + fields + " WHERE " + where; 
    } else { // insert
      fields += ",DateCreated=NOW()";
      sql = "INSERT INTO " + tableTo + " SET " + fields;
    }

    setDatabaseData(database_des); // just in case for the other methods
    
    updateSql_v2(database_des, sql);

  }
  
  private String getWhere() {
    String srcPrimKeyValue = ColumnData.getValueOfPrimaryKey(columnData_src);
    String desPrimKeyColName = ColumnData.getColumnNameOfPrimaryKey(columnData_des);
    String where = "(`" + desPrimKeyColName + "`='" + srcPrimKeyValue + "')";
    return where;
  }
  
  private String getFields() {
    String sql = "";
    for (int i=0; i < columnData_des.length; i++) {
      String column = columnData_des[i].getColumnName();
      String value = columnData_des[i].getValue();
      
      sql += "`" + column + "`='" + value + "'";
      if (i < columnData_des.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  private String getFields_OneToMany() {
    
    String sql = "";
    
    // one to many relationship
    sql += oneToMany_RelationshipSql + ", ";
    
    // mapped fields
    for (int i=0; i < columnData_des_oneToMany.length; i++) {
      String column = columnData_des_oneToMany[i].getColumnName();
      String value = columnData_des_oneToMany[i].getValue();
      
      sql += "`" + column + "`='" + value + "'";
      if (i < columnData_des_oneToMany.length -1) {
        sql += ",";
      }
    }
    
    return sql;
  }
  
  private String getFields_OneToMany_Hard(HashMap<String, String> hardOneToMany, int type) {
    String sep = "";
    if (type == 1) {
      sep = ",";
    } else if (type == 2) {
      sep = " AND ";
    }
    
    String sql = "";
    Iterator iterator = (hardOneToMany.keySet()).iterator();
    int i=0;
    while (iterator.hasNext()) {
      String key = iterator.next().toString();
      String value = hardOneToMany.get(key).toString();
      if (i > 0) {
        sql += sep;
      }
      sql += "`" + key + "`='" + value + "'";
      i++;
    }// end of while
    return sql;
  }
  
  /**
   * get dest values
   */
  private void getDestinationValuesToCompareWith(ColumnData[] src, ColumnData[] des) {
    
    // TODO asumming that the primary key is the same
    String srcPrimKeyValue = ColumnData.getValueOfPrimaryKey(src);
    String desPrimKeyColName = ColumnData.getColumnNameOfPrimaryKey(des);
    
    String where = "(`" + desPrimKeyColName + "`='" + srcPrimKeyValue + "')";
    
    oneToMany_RelationshipSql = "`" +desPrimKeyColName + "`='" + srcPrimKeyValue + "'"; 
    
    String sql = "SELECT * FROM " + tableTo + " WHERE " + where + ";";
    
    try {
      Connection conn = database_des.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        for (int i=0; i < des.length; i++) {
          String value = result.getString(des[i].getColumnName());
          des[i].setValue(value);
        }
        
      }
      result.close();
      select.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }
  
  private void getDestinationValuesToCompareWith_OneToMany(ColumnData[] src, ColumnData[] des) {
    
    // TODO - is the primary different in one to many table?
    String srcPrimKeyValue = ColumnData.getValueOfPrimaryKey(columnData_src);
    String desPrimKeyColName = ColumnData.getColumnNameOfPrimaryKey(columnData_des);
    String where = "(`" + desPrimKeyColName + "`='" + srcPrimKeyValue + "')";
    
    for (int i=0; i < src.length; i++) {
      getDestinationValuesToCompareWith_OneToMany(where, src[i], des[i]);
    }
    
  }

  private void getDestinationValuesToCompareWith_OneToMany(String where, ColumnData src, ColumnData des) {
  
    String sql = "SELECT * FROM " + des.getTable() + " WHERE " + where + ";";
    
    try {
      Connection conn = database_des.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        
        String value = result.getString(des.getColumnName());
        des.setValue(value);
        
      }
      result.close();
      select.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }

  private int doIdentsExistAlready(DatabaseData databaseData) {
    
    setDatabaseData(databaseData);
    
    ColumnData primaryKey = getPrimaryKey_MySql_v2(database_des, tableTo);
    
    String sql = "Select `" + primaryKey.getColumnName() + "` FROM " + tableTo + " WHERE "  + getSqlIdent();
    
    int id = getQueryInt_v2(database_des, sql);
    
    return id;
  }
  
  private String getSqlIdent() {
    
    String sql = "";
    int is = 0;
    for (int i=0; i < columnData_des.length; i++) {
      if (columnData_des[i].getIsPrimaryKey() == true) {
        
        if (is > 1) {
          sql += " AND ";
        }
        
        sql += "(`" + columnData_des[i].getColumnName() + "`='" + SQLProcessing.escapeForSql(columnData_des[i].getValue()) + "')";
        
        is++;
      }
    }
    
    return sql;
  }
  
  private void merge() {
    
    // TODO - what no values exist on the other end?
    
    for (int i=0; i < columnData_src.length; i++) {
      
      boolean onlyOverwriteBlank = columnData_des[i].getOverwriteWhenBlank();
      boolean onlyOverwriteZero = columnData_des[i].getOverwriteWhenZero();
      
      String desValue = columnData_des[i].getValue();
      
      if (desValue == null) {
        desValue = "";
      }
      
      // TODO - is zero always considered a blank, maybe not?
      if ( (onlyOverwriteBlank == true && (desValue.equals("null") | desValue.length() == 0)) | 
          (onlyOverwriteZero == true && (desValue.equals("null") | desValue.length() == 0 | desValue.equals("0"))) ) { // write when blank
        columnData_des[i].setValue(columnData_src[i].getValue());
      } 
      
    }
   
  }
  
  private void merge_OneToMany() {
    
    // TODO - what no values exist on the other end?
    
    for (int i=0; i < columnData_src_oneToMany.length; i++) {
      
      boolean onlyOverwriteBlank = columnData_des_oneToMany[i].getOverwriteWhenBlank();
      boolean onlyOverwriteZero = columnData_des[i].getOverwriteWhenZero();
      
      String desValue = columnData_des_oneToMany[i].getValue();
      
      if (desValue == null) {
        desValue = "";
      }
      
      if ( (onlyOverwriteBlank == true && (desValue.equals("null") | desValue.length() == 0)) | 
          (onlyOverwriteZero == true && (desValue.equals("null") | desValue.length() == 0 | desValue.equals("0"))) ) { // write when blank
        columnData_des_oneToMany[i].setValue(columnData_src_oneToMany[i].getValue());
      } 
      
    }
   
  }
  
}
