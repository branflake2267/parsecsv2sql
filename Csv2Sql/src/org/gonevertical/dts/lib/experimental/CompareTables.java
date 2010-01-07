package org.gonevertical.dts.lib.experimental;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnmulti.ColumnLibFactory;
import org.gonevertical.dts.lib.sql.querylib.QueryLib;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;

public class CompareTables {

	private long limitOffset = 100;
	
	//source database settings
  private DatabaseData database_src = null;
  
  // destination database settings
  private DatabaseData database_des = null;
	
  private QueryLib ql_src = null;
  private TransformLib tl_src = null;
  private ColumnLib cl_src = null;
  private QueryLib ql_des = null;
  private TransformLib tl_des = null;
  private ColumnLib cl_des = null;
  
  private ColumnData[] columnData_src = null;
  private ColumnData[] columnData_des = null;
  
  private String tableLeft = null;
  private String tableRight = null;
  
  private String srcWhere = null;
  
  
	public CompareTables(DatabaseData database_src, DatabaseData database_dest) {
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
	
  public void checkTablesCounts(String[] tables) {
  	for (int i=0; i < tables.length; i++) {
  		checkTableCount(tables[i]);
  	}
  }
  
  public void checkTableCounts(String leftTable, String rightTable) {
  	checkTableCount(leftTable, rightTable);
  }
  
  private void checkTableCount(String leftTable, String rightTable) {
  	
  	String where = "";
  	if (srcWhere != null) {
  		where = " WHERE " + srcWhere;
  	}
  	
  	String sqlL = "SELECT COUNT(*) AS t FROM " + leftTable + where;
  	String sqlR = "SELECT COUNT(*) AS t FROM " + rightTable + where;
  	
  	long left = ql_src.queryLong(database_src, sqlL);
  	long right = ql_des.queryLong(database_des, sqlR);
  
  	String match = "";
  	if (left == right) {
  		 match = "TRUE";
  	} else {
  		match = "FALSE";
  	}
  	System.out.println("LeftTable: " + leftTable + " left: " + left + " RightTable: "+ rightTable + " right: " + right + " " + match + " offby: " + (left-right));
  }
  
  private void checkTableCount(String table) {
  	String sql = "SELECT COUNT(*) AS t FROM " + table;
  	
  	long left = ql_src.queryLong(database_src, sql);
  	long right = ql_des.queryLong(database_des, sql);
  
  	String match = "";
  	if (left == right) {
  		 match = "TRUE";
  	} else {
  		match = "FALSE";
  	}
  	System.out.println("table: " + table + " left: " + left + " right: " + right + " " + match + " offby: " + (left-right));
  }
	
  public void compareTableData(String tableLeft, String tableRight) {
  	this.tableLeft = tableLeft;
  	this.tableRight = tableRight;
  	
  	setColumnData_All();
  	
  	processSrc();
  }
  
  private String getSrcWhere() {
    String s = "";
    if (srcWhere != null && srcWhere.length() > 0) {
      s = " AND " + srcWhere;
    }
    return s; 
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
  
  private void processSrc() {
    String sql = "SELECT COUNT(*) AS t FROM `" + tableLeft + "`;";
    System.out.println("sql" + sql);
    long total = ql_src.queryLong(database_src, sql);
    long lim = limitOffset;
    long totalPages = (total / lim);
    if (totalPages == 0) {
      totalPages = 1;
    }
    long offset = 0;
    long limit = 0;
    for (int i=0; i < totalPages; i++) {
      if (i==0) {
        offset = 0;
        limit = lim;
      } else {
        offset = ((i + 1 )* lim) - lim;
        limit = lim;
      }
      processSrc(offset, limit);
    }
  }
  
  private void processSrc(long offset, long limit) {

    ColumnData primKey = cl_src.getPrimaryKey_ColumnData(columnData_src);
    String where = "";
    if (primKey != null) {
      where = "WHERE " + primKey.getColumnName() + " != '' AND " + primKey.getColumnName() + " IS NOT NULL";
    }
    
    String columnCsv = cl_src.getSql_Names_WSql(columnData_src, null);
    
    String sql = "";
    sql = "SELECT " + columnCsv + " FROM " + tableLeft + " ";
    sql += where;
    sql += getSrcWhere();
    sql += " LIMIT " + offset + ", " + limit + ";";
    
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
  
  private void process() {
    
  	getDestinationValuesForComparison();
  	
  	String s = "";
    for (int i=0; i < columnData_src.length; i++) {
      
    		String leftValue = columnData_src[i].getValue();
      	String rightValue = columnData_des[i].getValue();
      
      	String match = "";
      	if (leftValue == null && rightValue == null) {
      		match = "TRUE";
      	} else if (leftValue == null | rightValue == null) {
      		match = "FALSE";
      	} else if (leftValue.equals(rightValue) == true) {
      		match = "TRUE";
      	} else {
      		match = "FALSE";
      	}
        
      	s += columnData_src[i].getName() + ": " + match + ", ";
      
    }
    System.out.println(s);
  }

  private void getDestinationValuesForComparison() {
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
  
  public void setWhere(String where) {
  	this.srcWhere = where;
  }
  
}
