package org.gonevertical.dts.data;

import java.util.ArrayList;
import java.util.Date;

public class ImportStatData {

	private long startTime;
	private long endTime;
	
	private long sqlNullCount = 0;
	private long sqlTotalCount = 0;
	private long sqlInsertCount = 0;
	private long sqlUpdateCount = 0;
	private long sqlAlterCount = 0;
	
	private long saveCount = 0;
	
	// count rows gone through
	private long rowCount = 0;
	
	private ArrayList<String> errors = new ArrayList<String>();

	public ImportStatData() {
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
	
	public void print() {
		long diff = endTime - startTime;
		System.out.println("start: " + startTime + " endTime: " + endTime + " diffTime: " + diff);
		System.out.println("rowCount: " + rowCount);
		System.out.println("sqlTotalCount: " + sqlTotalCount);
		System.out.println("sqlAlterCount: " + sqlAlterCount);
		System.out.println("sqlInsertCount: " + sqlInsertCount);
		System.out.println("sqlUpdateCount: " + sqlUpdateCount);
		System.out.println("sqlNullCount: " + sqlNullCount);
		System.out.println("saveCount: " + saveCount);
	}
	
	
	
	
	
	
	
	
	
}
