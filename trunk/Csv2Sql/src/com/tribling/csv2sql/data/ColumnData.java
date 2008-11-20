package com.tribling.csv2sql.data;

public class ColumnData {

	public String column;
	
	public String type;
	
	public int length;
	
	
	public ColumnData() {
	}
	
	/**
	 * compare columns to see if they values will all fit
	 * @param columns
	 * @param col
	 * @param val
	 * @return
	 */
	public boolean compare(ColumnData[] columns, String[] col, String[] val) {
		
		return false;
	}
}
