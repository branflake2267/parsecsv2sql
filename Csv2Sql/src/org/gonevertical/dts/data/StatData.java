package org.gonevertical.dts.data;

import java.util.ArrayList;
import java.util.Date;

public class StatData {

	/**
	 * track micro time start
	 */
	private long startTime;
	
	/**
	 * track micro time end
	 */
	private long endTime;
	
	/**
	 * how many times sql statement was null (ERROR)
	 */
	private long sqlNullCount = 0;
	
	/**
	 * total sql statements used
	 */
	private long sqlTotalCount = 0;
	
	/**
	 * how many times insert was used
	 */
	private long sqlInsertCount = 0;
	
	/**
	 * how many times update was used
	 */
	private long sqlUpdateCount = 0;
	
	/**
	 * how many times alter used
	 */
	private long sqlAlterCount = 0;
	
	/**
	 * how many times the sql update/insert was done
	 */
	private long saveCount = 0;
	
	/**
	 * rows gone through
	 */
	private long rowCount = 0;
	
	/**
	 * keep track of errors
	 */
	private ArrayList<String> errors = new ArrayList<String>();

	/**
	 * how many lines in the file
	 */
	private long fileLineCount = 0;
	
	public StatData() {
	}
	
	public void start() {
		startTime = new Date().getTime();
	}
	
	public void end() {
		endTime = new Date().getTime();
	}
	
	public void setTrackSql(String sql) {
		
		if (sql == null) {
			sqlNullCount ++;
			return;
		}
		
		sqlTotalCount++;
		
		if (sql.toLowerCase().contains("insert") ==  true) {
			sqlInsertCount++;
		} else if (sql.toLowerCase().contains("update") == true) {
			sqlUpdateCount++;
		} else if (sql.toLowerCase().contains("alter") == true) {
			sqlAlterCount++;
		}
		
	}
	
	public void setSaveCount() {
		saveCount++;
	}
	
	public void setAddRowCount() {
		rowCount++;
	}
	
	public void setTrackError(String error) {
		errors.add(error);
	}
	
	public void setFileLineCount(long fileLineCount) {
		this.fileLineCount = fileLineCount;
	}
	
	/**
	 * output to String
	 * 
	 * TODO maybe change this toString()
	 */
	public void print() {
		long diff = endTime - startTime;
		System.out.println("startTime: " + startTime + " endTime: " + endTime + " diffTime: " + diff);
		System.out.println("fileLineCount: " + fileLineCount);
		System.out.println("rowCount: " + rowCount);
		System.out.println("sqlStatementsTotalCount: " + sqlTotalCount);
		System.out.println("sqlStatementAlterCount: " + sqlAlterCount);
		System.out.println("sqlStatementInsertCount: " + sqlInsertCount);
		System.out.println("sqlStatementUpdateCount: " + sqlUpdateCount);
		System.out.println("sqlStatementNullCount: " + sqlNullCount);
		System.out.println("saveStatementsCount (update/insert): " + saveCount);	
	}
	
	private void createLoggingTable() {
		
	}
	
	private void writeToTable() {
		
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
  }

	public long getFileLineCount() {
		return fileLineCount;
  }
	
	
	
	
	
}
