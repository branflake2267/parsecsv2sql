package org.gonevertical.dts.v2;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.lib.datetime.DateTimeParser;
import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnmulti.ColumnLibFactory;
import org.gonevertical.dts.lib.sql.querylib.QueryLib;
import org.gonevertical.dts.lib.sql.querymulti.QueryLibFactory;
import org.gonevertical.dts.lib.sql.transformlib.TransformLib;
import org.gonevertical.dts.lib.sql.transformmulti.TransformLibFactory;

public class Optimise_FormatColumn implements Runnable {

	public static final int FORMAT_INT = 1;
	public static final int FORMAT_DATETIME = 2;
	
	private int formatType = 0;
	
	 //supporting libraries
  private QueryLib ql = null;
  private TransformLib tl = null;
  private ColumnLib cl = null;
	
	private DestinationData_v2 destinationData;
	
	//used to examine the value if its a date, and it can transform it
  private DateTimeParser dtp = new DateTimeParser();
	private ColumnData columnData;
	private long offset;
	private long limit;
	
	private long index = 0;

	public Optimise_FormatColumn() {
	}

  /**
   * guice injects the libraries needed for the database
   */
  private void setSupportingLibraries() {
    // get query library
    ql = QueryLibFactory.getLib(destinationData.databaseData.getDatabaseType());
    
    // get column library
    cl = ColumnLibFactory.getLib(destinationData.databaseData.getDatabaseType());
    
    // get tranformation library
    tl = TransformLibFactory.getLib(destinationData.databaseData.getDatabaseType());
  }

	public void setData(DestinationData_v2 destinationData, int formatType, ColumnData columnData, long offset, long limit, long total) {
		this.destinationData = destinationData;
		this.formatType = formatType;
		this.columnData = columnData;
		this.offset = offset;
		this.limit = limit;
		this.index = total;
		setSupportingLibraries();
	}
  
	public void run() {
		
		if (formatType == 0) {
			return;
		} else if (formatType == FORMAT_INT) {
			formatColumn_ToInt();
		} else if (formatType == FORMAT_DATETIME) {
			formatColumn_ToDateTime();
		}
		
	}
	
	private void formatColumn_ToInt() {

    ColumnData cpriKey = tl.queryPrimaryKey(destinationData.databaseData, columnData.getTable());
    
    String sql = "SELECT " + cpriKey.getColumnName() + ", `" + columnData.getColumnName() + "` " +
        "FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "` LIMIT " + offset + ", " + limit + ";"; 
    
    System.out.println(sql);
    
    try {
      Connection conn = destinationData.databaseData.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        cpriKey.setValue(Integer.toString(result.getInt(1)));
        columnData.setValue(result.getString(2));
        updateColumn_Int(cpriKey, columnData);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
  }
  
  private void updateColumn_Int(ColumnData cpriKey, ColumnData columnData) {

    String before = columnData.getValue();
    
    String value = columnData.getValue();
    if (value == null) {
      value = "0";
    } else if (value.trim().length() == 0) {
      value = "0";
    } else {
      try {
        // change (1234) to negative
        if (value != null && value.matches("[\\(].*[\\)]")) {
          value = value.replaceAll("[\\)\\(]", "");
          value = "-" + value;
        }
        // take out all non digit characters except . - and 0-9
        if (value != null) {
          value = value.replaceAll("[^0-9\\.\\-]", ""); 
        }
        BigDecimal bd = new BigDecimal(value);
        value = bd.toString();
      } catch (NumberFormatException e) {
        value = "0";
      } 
    }

    columnData.setValue(value);
    
    destinationData.debug(index + ". column: " + columnData.getColumnName() + " (is an int) was before: " + before + " after: " + value);

    // is there room for the transformation values
    columnData.alterColumnSizeBiggerIfNeedBe(destinationData.databaseData);
    
    ColumnData[] c = new ColumnData[2];
    c[0] = cpriKey;
    c[1] = columnData;

    String sql = cl.getSql_Update(c);
    ql.update(destinationData.databaseData, sql);
    
    index--;
  }
  
	private void formatColumn_ToDateTime() {

		ColumnData cpriKey = tl.queryPrimaryKey(destinationData.databaseData, columnData.getTable());

		String sql = "SELECT " + cpriKey.getColumnName() + ", `" + columnData.getColumnName() + "` " +
			"FROM `" + destinationData.databaseData.getDatabase() + "`.`" + columnData.getTable() + "` LIMIT " + offset + ", " + limit + ";";
		
		System.out.println(sql);
		
		try {
			Connection conn = destinationData.databaseData.getConnection();
			Statement select = conn.createStatement(); 
			ResultSet result = select.executeQuery(sql);
			while (result.next()) {
				cpriKey.setValue(Integer.toString(result.getInt(1)));
				columnData.setValue(result.getString(2));
				updateColumn_Date(cpriKey, columnData);
			}
			select.close();
			select = null;
			result.close();
			result = null;
			conn.close();
			conn = null;
		} catch (SQLException e) {
			System.err.println("Mysql Statement Error:" + sql);
			e.printStackTrace();
		}
	}


	private void updateColumn_Date(ColumnData cpriKey, ColumnData columnData) {
		String datetime = columnData.getValue();
		String tranformed = dtp.getDateMysql(datetime);

		if (datetime == null) {
			tranformed = null;
		} else if (datetime.trim().length() == 0) {
			tranformed = null;
		} 

		columnData.setValue(tranformed);
		destinationData.debug(index + ". column: " + columnData.getColumnName() + " datetime before: " + datetime + " after: " + tranformed);

		// is there room for the transformation values
		columnData.alterColumnSizeBiggerIfNeedBe(destinationData.databaseData);

		ColumnData[] c = new ColumnData[2];
		c[0] = cpriKey;
		c[1] = columnData;

		String sql = cl.getSql_Update(c);
		ql.update(destinationData.databaseData, sql);
		
		index--;
	}
	
}
