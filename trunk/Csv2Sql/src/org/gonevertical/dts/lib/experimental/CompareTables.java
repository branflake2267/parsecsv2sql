package org.gonevertical.dts.lib.experimental;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnmulti.ColumnLibFactory;
import org.gonevertical.dts.lib.sql.querylib.QueryLib;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;
import org.gonevertical.dts.v2.CsvProcessing_v2;

public class CompareTables {

	private Logger logger = Logger.getLogger(CompareTables.class);
	
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
  
  private boolean doesntMatch = false;

	private ColumnData[] columnIdentities;

	private ColumnData[] columnSkip;

	private String sqlOrderBy;

	private boolean failure = false;
	private int failureCount = 0;
  
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
  
  public void checkTableCounts(String tableLeft, String tableRight) {
  	failure = false;
  	failureCount = 0;
  	checkTableCount(tableLeft, tableRight);
  }
  
  private void checkTableCount(String leftTable, String rightTable) {
  	doesntMatch = false;
  	
  	String where = "";
  	if (srcWhere != null) {
  		where = " WHERE " + srcWhere;
  	}
  	
  	String sqlL = "SELECT COUNT(*) AS t FROM " + leftTable + where;
  	String sqlR = "SELECT COUNT(*) AS t FROM " + rightTable + where;
  	
  	System.out.println(sqlL);
  	
  	long left = ql_src.queryLong(database_src, sqlL);
  	long right = ql_des.queryLong(database_des, sqlR);
  
  	String match = "";
  	if (left == right) {
  		 match = "TRUE";
  		 doesntMatch = false;
  	} else {
  		match = "FALSE";
  		doesntMatch = true;
  	}
  	if (doesntMatch == true) {
  		logger.error("CompareTables.checkTableCount(): " +
  				"LeftTable: " + leftTable + " left: " + left + " RightTable: "+ rightTable + " right: " + right + " " + match + " offby: " + (left-right));
  	}
  	
  	logger.info("CompareTables.checkTableCount(): " +
				"LeftTable: " + leftTable + " left: " + left + " RightTable: "+ rightTable + " right: " + right + " " + match + " offby: " + (left-right));
  }
  
  private void checkTableCount(String table) {
  	doesntMatch = false;
  	
  	String sql = "SELECT COUNT(*) AS t FROM " + table;
  	
  	long left = ql_src.queryLong(database_src, sql);
  	long right = ql_des.queryLong(database_des, sql);
  
  	String match = "";
  	if (left == right) {
  		 match = "TRUE";
  		 doesntMatch = false;
  	} else {
  		match = "FALSE";
  		doesntMatch = true;
  	}
  	logger.info("table: " + table + " left: " + left + " right: " + right + " " + match + " offby: " + (left-right));
  }
	
  public void compareTableData(String tableLeft, String tableRight, ColumnData[] columnIdentities, ColumnData[] columnSkip) {
  	this.columnIdentities = columnIdentities;
  	this.columnSkip = columnSkip;
  	compareTableData(tableLeft, tableRight);
  }
  
  public void compareTableData(String tableLeft, String tableRight) {
  	this.tableLeft = tableLeft;
  	this.tableRight = tableRight;
  	
  	failure = false;
  	failureCount = 0;
  	
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
    
    // skip these columns
    if (columnSkip != null) {
    	columnData_src = cl_src.prune(columnData_src, columnSkip);
    }
    
    columnData_des = new ColumnData[columnData_src.length];
    for(int i=0; i < columnData_src.length; i++) {
    	
    	// skip these    	
      columnData_des[i] = new ColumnData();
      columnData_des[i] = (ColumnData) columnData_src[i].clone();
      columnData_des[i].setTable(tableRight);
    
      // use identities instead of primary key
      if (columnIdentities != null) {
        int index = cl_src.searchColumnByName_UsingComparator(columnIdentities, columnData_des[i]);
        if (index >= 0) {
        	columnData_src[i].setIdentity(true);
        }
      }
    }
    
    
  }
  
  private void processSrc() {
    
  	String sql = "SELECT COUNT(*) AS t FROM `" + tableLeft + "`;";
    
    logger.trace("sql" + sql);
    
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
    
    String where = "WHERE (1=1) ";
    if (columnIdentities != null) {
    	where += "";
    	
    } else  if (primKey != null) {
      where += "AND " + primKey.getColumnName() + " != '' AND " + primKey.getColumnName() + " IS NOT NULL";
    }
    
    String columnCsv = cl_src.getSql_Names_WSql(columnData_src, null);
    
    String sql = "";
    sql = "SELECT " + columnCsv + " FROM " + tableLeft + " ";
    sql += where;
    sql += getSrcWhere();
    sql += getOrderBy();
    sql += " LIMIT " + offset + ", " + limit + ";";
    
    logger.info(sql);
    
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
  
  private String getOrderBy() {
	  String s = "";
	  if (sqlOrderBy != null) {
	  	s = " " + sqlOrderBy + " ";
	  }
	  return s;
  }

	private void process() {
    
  	getDestinationValuesForComparison();
  	
  	String s = "";
  	boolean overallRowMatch = true;
    for (int i=0; i < columnData_src.length; i++) {
      
    		String leftValue = columnData_src[i].getValue();
      	String rightValue = columnData_des[i].getValue();
      
      	boolean bmatch = false;
      	if (leftValue == null && rightValue == null) {
      		bmatch = true;
      		
      	} else if (leftValue == null | rightValue == null) {
      		bmatch = false;
      		
      	} else if (leftValue.equals(rightValue) == true) {
      		bmatch = true;
      		
      	} else {
      		bmatch = false;
      	}
        
      	s += "(" + columnData_src[i].getName() + ":" + leftValue + "==" + rightValue + ":" + bmatch + "), ";
      	
      	if (bmatch == false) {
      		overallRowMatch = false;
      	}
      
    }
    
    logger.info("Overall: " + overallRowMatch + " ::: " + s);
 
    if (overallRowMatch == false) {
    	failure = true;
    	failureCount++;
    	logger.error("CompareTables.process(): Data is not matching. Overall: " + overallRowMatch + " ::: " + s);
    }
  
  }

  private void getDestinationValuesForComparison() {
  	
    String srcPrimKeyValue = cl_src.getPrimaryKey_Value(columnData_src);
    String desPrimKeyColName = cl_des.getPrimaryKey_Name(columnData_des);
    
    String where = "";
    if (columnIdentities != null) {
    	where = getSqlWhereForIdents();
    } else {
    	where = "(" + desPrimKeyColName + "='" +  ql_src.escape(srcPrimKeyValue) + "')";
    }
    
    String sql = "";
    
    sql = "SELECT * FROM " + tableRight + " WHERE " + where;
      
    
    logger.trace("getDestinationValuesToCompareWith(): " + sql);
    
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
  
  private String getSqlWhereForIdents() {
	  String sql = cl_src.getSql_IdentitiesWhere(columnData_src);
	  return sql;
  }

	public void setWhere(String where) {
  	this.srcWhere = where;
  }
  
  public boolean getDoesItMatch() {
  	return doesntMatch;
  }

	public void setOrderBy(String sqlOrderBy) {
	  this.sqlOrderBy = sqlOrderBy;
  }

	public boolean getFailure() {
	  return failure;
  }
	
	public int getFailureCount() {
		return failureCount;
	}
  
}
