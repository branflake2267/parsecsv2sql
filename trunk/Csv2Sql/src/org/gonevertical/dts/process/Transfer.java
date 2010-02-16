package org.gonevertical.dts.process;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.data.FieldData;
import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnmulti.ColumnLibFactory;
import org.gonevertical.dts.lib.sql.querylib.QueryLib;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;


/**
 * transfer data from one table to another
 * 
 * TODO - do i want to add multple idents as the solution of transfering from source with no autoincrement?
 * TODO - how to transfer from source with no auto increment
 * TODO - be able transform strings to sentence case  HELLO THERE to Hello there.
 * TODO - move 100page to 1000 depending on column size
 * 
 * @author design
 *
 */
public class Transfer {

  // what kind of transfer process is going on
  public static final int MODE_TRANSFER_ALL = 1;
  public static final int MODE_TRANSFER_ONLY = 2;
  public static final int MODE_MASH = 3; // mash to it self
  private int mode = 0;
  
  //supporting libraries
  private QueryLib ql_src = null;
  private TransformLib tl_src = null;
  private ColumnLib cl_src = null;
  private QueryLib ql_des = null;
  private TransformLib tl_des = null;
  private ColumnLib cl_des = null;
  
  // source database settings
  private DatabaseData database_src = null;
  
  // destination database settings
  private DatabaseData database_des = null;
  
  // transfering from table to table
  private String tableLeft = null;
  private String tableRight = null;
  
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
  
  // filter your query
  private String srcWhere = null;
  
  // this will skip comparing dest columns - on an update it will update with out overwrite policy
  private boolean compareDestValues = true;
  
  // total count query
  private long total = 0;
  
  // query this many records at a time
  private long limitOffset = 100;
	
  // at index
  private long index = 0;
  
  /**
   * Transfer data object setup 
   * 
   * @param database_src
   * @param database_dest
   */
  public Transfer(DatabaseData database_src, DatabaseData database_dest) {
    this.database_src = database_src;
    this.database_des = database_dest;
    setSupportingLibraries();
  }
  
  /**
   * guice injects the libraries needed for the database
   */
  private void setSupportingLibraries() {
    ql_src = QueryLibFactory.getLib(database_src.getDatabaseType());
    cl_src = ColumnLibFactory.getLib(database_src.getDatabaseType());
    tl_src = TransformLibFactory.getLib(database_src.getDatabaseType());
    ql_des = QueryLibFactory.getLib(database_des.getDatabaseType());
    cl_des = ColumnLibFactory.getLib(database_des.getDatabaseType());
    tl_des = TransformLibFactory.getLib(database_des.getDatabaseType());
  }

  /**
   * set up tables from to
   * 
   * @param fromTable
   * @param toTable
   */
  public void transferAllFields(String fromTable, String toTable) {
    this.mode = MODE_TRANSFER_ALL;
    this.tableLeft = fromTable;
    this.tableRight = toTable;
    start();
  }
  
  public void transferOnlyMappedFields(String fromTable, String toTable, FieldData[] mappedFields) {
    this.mode = MODE_TRANSFER_ONLY;
    this.tableLeft = fromTable;
    this.tableRight = toTable;
    this.mappedFields = mappedFields;
    start();
  }
  
  public void mashSrc(String fromTable, String toTable, FieldData[] mappedFields) {
    this.mode = MODE_MASH;
    this.tableLeft = fromTable;
    this.tableRight = toTable;
    this.mappedFields = mappedFields;
    start();
  }
  
  /**
   * set a custom where query for the source table (no need for WHERE)
   * 
   * @param srcWhere
   */
  public void setWhere(String srcWhere) {
    this.srcWhere = srcWhere;
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
    this.tableLeft = fromTable;
    this.tableRight = toTable;
    this.mappedFields = mappedFields;
    this.oneToMany  = oneToMany;
    start();
  }
  
  /**
   * prep the objects needed
   */
  private void start() {
    
    index = 0;
    
    if (mode == MODE_TRANSFER_ALL) {
      setColumnData_All();
    } else if (mode == MODE_TRANSFER_ONLY) {
      setColumnData();
    } else if (mode == MODE_MASH) {
      setColumnData();
    }
    
    createDestTable();
    
    createColumns();
    
    if (mode == MODE_TRANSFER_ALL) {
      processSrc();
    } else if (mode == MODE_TRANSFER_ONLY) {
      processSrc();
    } else if (mode == MODE_MASH) {
      processSrc_Mash();
    }
    
  }

  /**
   * if dest table doesn't exist create it
   */
  private void createDestTable() {
    String primaryKeyName = cl_src.getPrimaryKey_Name(columnData_src);
    tl_des.createTable(database_des, tableRight, primaryKeyName);
  }
  
  private void createColumns() {
    
    for (int i=0; i < columnData_src.length; i++) {
      tl_des.createColumn(database_des, columnData_des[i]);
    }
    
  }
  
  private void setColumnData_All() {
    columnData_src = tl_src.queryColumns(database_src, tableLeft, null);
    
    columnData_des = new ColumnData[columnData_src.length];
    for(int i=0; i < columnData_src.length; i++) {
      columnData_des[i] = new ColumnData();
      columnData_des[i] = (ColumnData) columnData_src[i].clone();
      columnData_des[i].setTable(tableRight);
    }
  }
  
  private String getSrcWhere() {
    String s = "";
    if (srcWhere != null && srcWhere.length() > 0) {
      s = " AND " + srcWhere;
    }
    return s; 
  }
  
  /**
   * TODO start with columndata instead of field data, b/c there is more overhead here than I need
   * TODO slim down column data public vars 
   */
  private void setColumnData() {
    
    columnData_src = new ColumnData[mappedFields.length];
    columnData_des = new ColumnData[mappedFields.length];
    for (int i = 0; i < mappedFields.length; i++) {
      columnData_src[i] = new ColumnData();
      columnData_des[i] = new ColumnData();
      
      columnData_src[i].setTable(tableLeft);
      columnData_src[i].setColumnName(mappedFields[i].sourceField);
      columnData_src[i].setIsPrimaryKey(mappedFields[i].isPrimaryKey);
      columnData_src[i].setOverwriteWhenBlank(mappedFields[i].onlyOverwriteBlank);
      columnData_src[i].setOverwriteWhenZero(mappedFields[i].onlyOverwriteZero);
      columnData_src[i].setRegex(mappedFields[i].regexSourceField);
      
      columnData_des[i].setTable(tableRight);
      columnData_des[i].setColumnName(mappedFields[i].destinationField);
      columnData_des[i].setIsPrimaryKey(mappedFields[i].isPrimaryKey);
      columnData_des[i].setOverwriteWhenBlank(mappedFields[i].onlyOverwriteBlank);
      columnData_des[i].setCase(mappedFields[i].changeCase);
    }

    if (oneToMany != null) {
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
    
  }
  
  private void processSrc() {
  	
  	String where = "";
  	if (srcWhere != null && srcWhere.length() > 0) {
  		where = " WHERE " + srcWhere;
  	}
  	
    String sql = "SELECT COUNT(*) AS t FROM " + tableLeft + " " + where;
    System.out.println("sql" + sql);
    total = ql_src.queryLong(database_src, sql);
    index = total;
    
    loopThroughPages();
  }
  
  private void loopThroughPages() {
    
    long lim = limitOffset;
    BigDecimal tp = new BigDecimal(0);
    if (total > 0) {
    	tp = new BigDecimal(total).divide(new BigDecimal(lim, MathContext.DECIMAL32), MathContext.DECIMAL32).setScale(0, RoundingMode.UP);	
    } else {
    	tp = new BigDecimal(1);
    }
    
    long offset = 0;
    long limit = 0;
    for (int i=0; i < tp.intValue(); i++) {
      if (i==0) {
        offset = 0;
        limit = lim;
      } else {
        offset = ((i + 1 )* lim) - lim;
        limit = lim;
      }
      
      processSrc(offset, limit);
      //System.out.println("offset: " + offset + " limit: " + limit);
    }
    
  }
  
  private void processSrc(long offset, long limit) {

    ColumnData primKey = cl_src.getPrimaryKey_ColumnData(columnData_src);
    String where = "";
    if (primKey != null) {
      where = "WHERE " + primKey.getColumnName() + " != '' AND " + primKey.getColumnName() + " IS NOT NULL";
    }
    
    String columnCsv = cl_src.getSql_Names_WSql(columnData_src, null);
    
    String columnCsv2 = "";
    if (columnData_src_oneToMany != null) {
      columnCsv2 = cl_src.getSql_Names_WSql(columnData_src_oneToMany, null);
      
      if (columnCsv2.length() > 0) {
        columnCsv2 = "," + columnCsv2;
      }
    }
    
    String sql = "";
    
    // Microsoft paging sucks real bad. Why don't they have a built in function, duh!
    if (database_src.getDatabaseType() == DatabaseData.TYPE_MSSQL) {
    	sql += "SELECT * FROM ( ";
    }
    
    sql += "SELECT ";
    
    if (database_src.getDatabaseType() == DatabaseData.TYPE_MSSQL) {
    	sql += "(ROW_NUMBER() OVER(ORDER BY " + primKey.getColumnName() + ")) AS Auto_RowNum, ";
    }
    
    sql += " " + columnCsv + " " + columnCsv2 + " ";
    sql += " FROM ";
    
    if (database_src.getDatabaseType() == DatabaseData.TYPE_MSSQL) {
    	sql += database_src.getDatabase() + ".";
    	sql += database_src.getTableSchema() + ".";
    }
    sql += "" + tableLeft + " ";
    
    sql += where;
    sql += getSrcWhere();
    
    if (database_src.getDatabaseType() == DatabaseData.TYPE_MYSQL) {
    	sql += " LIMIT " + offset + ", " + limit + ";";
    }
    
    if (database_src.getDatabaseType() == DatabaseData.TYPE_MSSQL) {
    	sql += " ) AS TableWithRows WHERE TableWithRows.Auto_RowNum >= " + offset + " AND TableWithRows.Auto_RowNum <= " + (offset + limit) + " ";
    }
    
    //if (database_src.getDatabaseType() == DatabaseData.TYPE_MSSQL) {
      //sql = sql.replaceAll("`", "");
    //}
    
    System.out.println("sql: " + sql);
    
    Connection conn = null;
    Statement select = null;
    try {
      conn = database_src.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        // get values 
        for (int i=0; i < columnData_src.length; i++) {
          String value = result.getString(columnData_src[i].getColumnName());
          columnData_src[i].setValue(value);
        }
        
        // one to many relationships processing
        if (columnData_src_oneToMany != null && columnData_src_oneToMany.length > 0) {
          for (int i=0; i < columnData_src_oneToMany.length; i++) {
            String value = result.getString(columnData_src_oneToMany[i].getColumnName());
            columnData_src_oneToMany[i].setValue(value);
          }
        }

        process();
      }
      result.close();
      select.close();
      conn.close();
      result = null;
      select = null;
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    } finally {
      conn = null;
      select = null;
    }
  }
  
  private void processSrc_Mash() {

  	String where = "";
  	if (srcWhere != null && srcWhere.length() > 0) {
  		where = " WHERE " + srcWhere;
  	}
  	
    String sql = "SELECT COUNT(*) AS t FROM `" + tableLeft + "`" + where;
    System.out.println("sql" + sql);
    total = ql_src.queryLong(database_src, sql);
    index = total;
    
    long lim = limitOffset;
    BigDecimal tp = new BigDecimal(0);
    if (total > 0) {
    	tp = new BigDecimal(total).divide(new BigDecimal(lim)).setScale(0, RoundingMode.UP);	
    } else {
    	tp = new BigDecimal(1);
    }
    
    long offset = 0;
    long limit = 0;
    for (int i=0; i < tp.intValue(); i++) {
      if (i==0) {
        offset = 0;
        limit = lim;
      } else {
        offset = ((i + 1 )* lim) - lim;
        limit = lim;
      }
      
      processSrc_Mash(offset, limit);
      //System.out.println("offset: " + offset + " limit: " + limit);
    }
    
  }
  
  private void processSrc_Mash(long offset, long limit) {

    ColumnData primKey = cl_des.getPrimaryKey_ColumnData(columnData_des);
    String where = "WHERE " + primKey.getColumnName() + " != '' AND " + primKey.getColumnName() + " IS NOT NULL";
    
    ColumnData keyDes = cl_des.getPrimaryKey_ColumnData(columnData_des);
    
    String sql = "";
    sql = "SELECT " + keyDes.getColumnName() + " FROM " + tableRight + " ";
    sql += where;
    sql += getSrcWhere();
    sql += " LIMIT " + offset + ", " + limit + ";";
    
    System.out.println("sql: " + sql);
    
    Connection conn = null;
    Statement select = null;
    try {
      conn = database_des.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        String getKeyValue = result.getString(1);
        processSrc_Mash(getKeyValue);
      }
      result.close();
      select.close();
      conn.close();
      result = null;
      select = null;
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }
  
  private void processSrc_Mash(String keyValueDes) {

    String columnCsv = cl_src.getSql_Names_WSql(columnData_src, null);
    
    ColumnData keySrc = cl_src.getPrimaryKey_ColumnData(columnData_src);
    //ColumnData keyDes = ColumnData.getPrimaryKey_ColumnData(columnData_des);
    
    String sql = "";
    sql = "SELECT " + columnCsv + " FROM " + tableLeft + " ";
    sql += "WHERE " + keySrc.getColumnName() + " = '" + keyValueDes + "' ";
    sql += getSrcWhere();
    
    System.out.println("sql: " + sql);
    
    Connection conn = null;
    Statement select = null;
    try {
      conn = database_src.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        // get values 
        for (int i=0; i < columnData_src.length; i++) {
          String value = result.getString(columnData_src[i].getColumnName());
          if (value != null && value.trim().length() == 0) {
            value = null;
          }
          columnData_src[i].setValue(value);
        }
        
        // one to many relationships processing
        if (columnData_src_oneToMany != null && columnData_src_oneToMany.length > 0) {
          for (int i=0; i < columnData_src_oneToMany.length; i++) {
            String value = result.getString(columnData_src_oneToMany[i].getColumnName());
            columnData_src_oneToMany[i].setValue(value);
          }
        }

        process();
      }
      result.close();
      select.close();
      conn.close();
      result = null;
      select = null;
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
  }
  
  /**
   * compare for overwrite checking
   */
  private void process() {
    
    // compare src values to the dst values, for overwrite policy
    getDestinationValuesForComparison();
    
    if (columnData_src_oneToMany != null && columnData_src_oneToMany.length > 0) {
      getDestinationValuesToCompareWith_OneToMany(columnData_src_oneToMany, columnData_des_oneToMany);
    }
    
    // merge src values to destination 
    merge();
    
    if (columnData_src_oneToMany != null && columnData_src_oneToMany.length > 0) {
      merge_OneToMany();
    }
           
    save();
    
    // save one to many
    if (columnData_src_oneToMany != null && columnData_src_oneToMany.length > 0) {
      saveOneToMany();
    }
  }
  
  private void saveOneToMany() {
    
   // does the value already exist?
   for (int i=0; i < columnData_des_oneToMany.length; i++) {
     
     long onetoId = getOneToManyId(columnData_des_oneToMany[i], hardOneToMany.get(i));
     
     saveOneToMany(onetoId, columnData_des_oneToMany[i], hardOneToMany.get(i));
     
   }
    
  }
  
  private void saveOneToMany(long onetoId, ColumnData columnData, HashMap<String,String> hardOneToMany) {
    
    String hardFields = getFields_OneToMany_Hard(hardOneToMany, 1);
    
    String datafields = getFields_OneToMany(columnData);
    if (datafields == null) {
      //System.out.println("saveOneToMany(): no one to many value to insert");
      return;
    }
    
    if (hardFields.length() > 0) {
      datafields += ", " + hardFields;
    }
    
    String sql = "";
    if (onetoId > 0) { // update
      
      datafields += ",DateUpdated=NOW()";
      String where = "(`" + oneToManyTablePrimaryKey.getColumnName() + "`='" + onetoId + "')";
      sql = "UPDATE " + columnData.getTable() + " SET " + datafields + " WHERE " + where;
      
    } else { // insert
      
      datafields += ",DateCreated=NOW()";
      sql = "INSERT INTO " + columnData.getTable() + " SET " + datafields;
      
    }
    System.out.println("\t\t" + sql);
    ql_des.update(database_des, sql);
  }

  private long getOneToManyId(ColumnData columnData, HashMap<String,String> hardOneToMany) {
    
    // get primary key
    oneToManyTablePrimaryKey = tl_des.queryPrimaryKey(database_des, columnData.getTable());
    
    String where = getOneToManySqlWhere(columnData, hardOneToMany);

    String sql = "SELECT " + oneToManyTablePrimaryKey.getColumnName() + " FROM " + columnData.getTable() + " WHERE " + where;
    
    //System.out.println("checking onetomany: " + sql);
    
    long id = ql_des.queryLong(database_des, sql);
    
    return id;
  }
  
  // TODO - this is MYSQL specific - make it not specific using guice
  private String getOneToManySqlWhere(ColumnData columnData, HashMap<String,String> hardOneToMany) {

    String whereHard = "";
    if (hardOneToMany.isEmpty() == false) {
      whereHard = " AND " + getFields_OneToMany_Hard(hardOneToMany, 2);
    }
    
    String where = getWhere() + " AND " +
        "(`" + columnData.getColumnName()+"`='" +  ql_src.escape(columnData.getValue()) + "') " + whereHard;
 
    return where;
  }

  private void save() {
    
    long id = doIdentsExistAlready(database_des);
    
    String fields = getFields();
    
    String where = getWhere();
    
    String sql = "";
    if (id > 0) { // update
      if (cl_des.doesColumnNameExist(columnData_des, "DateUpdated") == false) {
        //fields += ",DateUpdated=NOW()";
      }
      sql = "UPDATE " + tableRight + " SET " + fields + " WHERE " + where; 
    } else { // insert
      if (cl_des.doesColumnNameExist(columnData_des, "DateCreated") == false) {
        //fields += ",DateCreated=NOW()";
      }
      sql = "INSERT INTO " + tableRight + " SET " + fields;
    }

    System.out.println(index + ". SAVE: " + sql);
    
    testColumnValueSizes(columnData_des);
    
    ql_des.update(database_des, sql, false);
    
    index--;
  }
  
  private void testColumnValueSizes(ColumnData[] columnData) {
    for (int i=0; i < columnData.length; i++) {
      columnData[i].alterColumnSizeBiggerIfNeedBe(database_des);
    }
  }
  
  private String getWhere() {
    String srcPrimKeyValue = cl_src.getPrimaryKey_Value(columnData_src);
    String desPrimKeyColName = cl_des.getPrimaryKey_Name(columnData_des);
    String where = "(`" + desPrimKeyColName + "`='" +  ql_src.escape(srcPrimKeyValue) + "')";
    return where;
  }
  
  private String getFields() {
    String sql = "";
    for (int i=0; i < columnData_des.length; i++) {
      String column = columnData_des[i].getColumnName();
      String value = columnData_des[i].getValue();
      
      if (value != null && value.trim().length() == 0) {
        value = null;
      }
      
      String svalue = "";
      if (value == null) {
        svalue = "NULL";
      } else {
        svalue = "'" +  ql_src.escape(value) + "'";
      }
      
      sql += "`" + column + "`=" + svalue;
      if (i < columnData_des.length -1) {
        sql += ",";
      }
    }
    return sql;
  }
  
  private String getFields_OneToMany(ColumnData columnData) {
    
    String sql = "";
    
    // one to many relationship defined into the hash table like userId=3134
    sql += oneToMany_RelationshipSql + ", ";
    
    // data fields
    String column = columnData.getColumnName();
    String value = columnData.getValue();
    sql += "`" + column + "`='" +  ql_src.escape(value) + "'";

    // don't insert blank data
    if (value == null || value.length() == 0) {
      sql = null;
    }
    
    return sql;
  }
  
  private String getFields_OneToMany_Hard(HashMap<String, String> hardOneToMany, int internaltype) {
    String sep = "";
    if (internaltype == 1) {
      sep = ",";
    } else if (internaltype == 2) {
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
      sql += "`" + key + "`='" + ql_src.escape(value) + "'";
      i++;
    }// end of while
    return sql;
  }
  
  /**
   * get dest values for comparison agianst the source data
   */
  private void getDestinationValuesForComparison() {

    //if (oneToMany_RelationshipSql == null && oneToMany != null) {
      setOneToManySqlRelationship();
    //}
    
    // this will skip comparing dest values, saving time, and just moving the data to dest regardless of overwrite policy
    if (compareDestValues == false) {
        for (int i=0; i < columnData_des.length; i++) {
          String s = null;
          columnData_des[i].setValue(s);
        }
      return;
    }
    
    // TODO do I need to keep getting the keys, or save them in class var
    // TODO asumming that the primary key is the same
    String srcPrimKeyValue = cl_src.getPrimaryKey_Value(columnData_src);
    String desPrimKeyColName = cl_des.getPrimaryKey_Name(columnData_des);
    
    String sql = "SELECT * FROM " + tableRight + " WHERE " +
      "(`" + desPrimKeyColName + "`='" +  ql_src.escape(srcPrimKeyValue) + "')";
    
    //System.out.println("getDestinationValuesToCompareWith(): " + sql);
    
    boolean b = false;
    Connection conn = null;
    Statement select = null;
    try {
      conn = database_des.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {

        for (int i=0; i < columnData_des.length; i++) {
          String value = result.getString(columnData_des[i].getColumnName());
          columnData_des[i].setValue(value);
          b = true;
        }
        
      }
      result.close();
      select.close();
      conn.close();
      result = null;
      select = null;
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
    if (b == false) {
      for (int i=0; i < columnData_des.length; i++) {
        String s = null;
        columnData_des[i].setValue(s);
      }
    }
  }
  
  private void setOneToManySqlRelationship() {
    String srcPrimKeyValue = cl_src.getPrimaryKey_Value(columnData_src);
    String desPrimKeyColName = cl_des.getPrimaryKey_Name(columnData_des);
    oneToMany_RelationshipSql = "`" + desPrimKeyColName + "`='" +  ql_src.escape(srcPrimKeyValue) + "'"; 
  }
  
  private void getDestinationValuesToCompareWith_OneToMany(ColumnData[] src, ColumnData[] des) {
    
    // TODO - is the primary different in one to many table?
    String srcPrimKeyValue = cl_src.getPrimaryKey_Value(columnData_src);
    String desPrimKeyColName = cl_des.getPrimaryKey_Name(columnData_des);
    String where = "(`" + desPrimKeyColName + "`='" +  ql_src.escape(srcPrimKeyValue) + "')";
    
    for (int i=0; i < src.length; i++) {
      getDestinationValuesToCompareWith_OneToMany(where, src[i], des[i]);
    }
  }

  private void getDestinationValuesToCompareWith_OneToMany(String where, ColumnData src, ColumnData des) {
  
    String sql = "SELECT * FROM " + des.getTable() + " WHERE " + where + ";";
    
    boolean b = false;
    Connection conn = null;
    Statement select = null;
    try {
      conn = database_des.getConnection();
      select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        String value = result.getString(des.getColumnName());
        des.setValue(value);
        b = true;
      }
      result.close();
      select.close();
      conn.close();
      result = null;
      select = null;
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
    if (b == false) {
      String s = null;
      des.setValue(s);
    }
    
  }

  private long doIdentsExistAlready(DatabaseData databaseData) {
   
    ColumnData primaryKey = tl_des.queryPrimaryKey(database_des, tableRight);
    
    if (primaryKey == null) {
      return 0;
    }
    
    String sql = "Select `" + primaryKey.getColumnName() + "` FROM " + tableRight + " WHERE "  + getSqlIdent();
    
   //System.out.println("\texist: " + sql);
    
    long id = ql_des.queryLong(database_des, sql);
    
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
        
        sql += "(`" + columnData_des[i].getColumnName() + "`='" + ql_src.escape(columnData_des[i].getValue()) + "')";
        
        is++;
      }
    }
    
    return sql;
  }
  
  private void merge() {
    
    for (int i=0; i < columnData_src.length; i++) {
      
      boolean onlyOverwriteBlank = columnData_des[i].getOverwriteWhenBlank();
      boolean onlyOverwriteZero = columnData_des[i].getOverwriteWhenZero();
      
      String desValue = columnData_des[i].getValue();
      
      // TODO get rid of this - needs testing
      if (desValue == null) {
        desValue = "";
      }
      
      // overwrite dest policy defined here
      if ( 
          (onlyOverwriteBlank == true && (desValue == null | desValue.length() == 0)) | 
          (onlyOverwriteZero == true && (desValue == null | desValue.length() == 0 | desValue.equals("0"))) 
         ) { // only overwrite when dest values are blank
        columnData_des[i].setValue(columnData_src[i].getValue());
        
      } else if (onlyOverwriteBlank == false | onlyOverwriteZero == false) {
        columnData_des[i].setValue(columnData_des[i].getValue());
      } else {
      	columnData_des[i].setValue(columnData_des[i].getValue());
      }
    }
   
  }
  
  private void merge_OneToMany() {
    
    // TODO - not sure if I need a merge
    // TODO - what no values exist on the other end?
    
    for (int i=0; i < columnData_src_oneToMany.length; i++) {
      
      boolean onlyOverwriteBlank = columnData_des_oneToMany[i].getOverwriteWhenBlank();
      boolean onlyOverwriteZero = columnData_des[i].getOverwriteWhenZero();
      
      String desValue = columnData_des_oneToMany[i].getValue();
      
      if (desValue == null) {
        desValue = "";
      }
      /* this won't apply here for now, b/c I am just looking to see if the value exists in the table if not write.
      if ( (onlyOverwriteBlank == true && (desValue.equals("null") | desValue.length() == 0)) | 
          (onlyOverwriteZero == true && (desValue.equals("null") | desValue.length() == 0 | desValue.equals("0"))) ) { // write when blank
        columnData_des_oneToMany[i].setValue(columnData_src_oneToMany[i].getValue());
      } 
      */
      columnData_des_oneToMany[i].setValue(columnData_src_oneToMany[i].getValue());
    }
   
    //System.out.println("Pause");
  }

  public void setCompareDestValues(boolean b) {
    this.compareDestValues = b;
  }
  
  public void setOffsetLimit(long limitOffset) {
  	this.limitOffset = limitOffset;
  }
  
}
