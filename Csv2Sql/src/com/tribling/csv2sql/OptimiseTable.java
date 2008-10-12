package com.tribling.csv2sql;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class OptimiseTable extends SQLProcessing {
	
	private int fieldType = 0;
	private int fieldLength = 0;

	/**
	 * constructor
	 */
	public OptimiseTable() {
	}
	
	protected void runOptimise(String[] columns) {

		if (optimise == false) {
			System.out.println("skipping optimising: destinationData.optimise = false");
			return;
		}
		
		// loop through each column
		loopThroughColumns(columns);
		
		System.out.println("Done Optimising");
	}

	public void runOptimise() {
		
		if (optimise == false) {
			System.out.println("skipping optimising: destinationData.optimise = false");
			return;
		}
		
		String [] columns = getColumns();
		
		columns = fixColumns(columns);
		
		// loop through each column
		loopThroughColumns(columns);
	}
	
	private void loopThroughColumns(String[] columns) {
		
		for(int i=0; i < columns.length; i++) {
			
			// reset
			fieldType = 0;
			fieldLength = 0;
			
			if (columns[i].equals("Period")) {
				System.out.println("pause");
			}
			
			queryColumn(columns[i]);
			
			String columnType = getColumnType();
			
			// alter column
			setAlterStatement(columns[i], columnType);
		}
		
	}
	
	private void setAlterStatement(String column, String columnType) {
		
		String modifyColumn = "[" + column + "] " + columnType;
		
		String alterQuery = "";
		if (databaseType == 1) {
			alterQuery = "ALTER TABLE `" + database + "`.`" + table + "` ALTER COLUMN " + modifyColumn;
		} else if (databaseType == 2) {
			alterQuery = "ALTER TABLE " + database + "." + tableSchema + "." + table + " ALTER COLUMN " + modifyColumn;
		}
		
		// TODO - only alter it if its different
		// TODO - skip after the first alter in directory format, first file sets the sizes
		setUpdateQuery(alterQuery);
		
	}
	
	private String getLimitQuery() {
		
		if (optimiseRecordsToExamine == 0) {
			optimiseRecordsToExamine = 500;
		}
		
		String s = "";
		if (databaseType == 1) {
			s = " LIMIT 0," + optimiseRecordsToExamine + " ";
		} else if (databaseType == 2) {
			s = " TOP " + optimiseRecordsToExamine + " ";
		}
		return s;
	}
	
	private void queryColumn(String column) {
		
		String query = "";
		if (databaseType == 1) {
			query = "SELECT " + column + " FROM " + database + "." + table + " " + getLimitQuery() + " ORDER BY RAND();";
		} else if (databaseType == 2) {
			query = "SELECT " + getLimitQuery() + " " + column + " FROM " + database + "." + tableSchema + "." + table + " ORDER BY NEWID();";
		}
		
		try {
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			while(result.next()) {
				String s = result.getString(1);
				examineField(s);
			}
			result.close();
		} catch (Exception e) {
			System.err.println("SQL Statement Error:" + query);
			e.printStackTrace();
		}
	}

	private void examineField(String s) {
		
		if (s == null) {
			s = "";
		}
		
		// whats the size of the field length
		setFieldLength(s);
		
		// NOTE: optimise only the varchar/text fields 
		// (this has to be done first anyway so we can alter varchar->date varchar->int...)
		if (optimiseTextOnly == true) {
			fieldType = 2;
			return;
		}
		
		boolean isInt = false;
		boolean isZero = false;
		boolean isDecimal = false;
		boolean isDate = false;
		boolean isText = false;
		boolean isEmpty = false;
		
		isDate = isDate(s);
		
		if (isDate == false) {
			isText = isText(s);
		}
		
		if (isText == false) {
			isInt = isInt(s);
		}
		
		if (isInt == true) {
			isZero = isIntZero(s);
		}
		
		if (isText == false) {
			isDecimal = isDecimal(s);
		}

		isEmpty = isEmpty(s);
		
		
		if (isDate == true) {  // date is first b/c it has text in it
			fieldType = 1; // date  
			
		} else if (isText == true && fieldType != 1) { 
			fieldType = 2; // varchar
			
		} else if (isInt == true && isZero == true && fieldType != 1 && fieldType != 2 && fieldType != 5) { // not date, text, decimal
			fieldType = 3; // int with zeros infront 0000123
			
		}  else if (isInt == true && fieldType != 2 && fieldType != 3 && fieldType != 5) { // not date,text,decimal
			fieldType = 4; // is Int
			
		} else if (isDecimal == true && fieldType != 1 && fieldType != 2) {
			fieldType = 5; // is decimal
			
		} else if (isEmpty == true && fieldType != 1 && fieldType != 2 && fieldType != 3 && fieldType != 4 && fieldType != 5) { // has nothing
			fieldType = 6;
		} else {
			fieldType = 7;
		}
		
		System.out.println("fieldType: " + fieldType + " Length: " + fieldLength + " Value::: " + s);
	}
	
	/**
	 * get ready to figure out how to alter the column
	 */
	private String getColumnType() {
		
		String columnType = null;
		
		if (databaseType == 1) {
			columnType = getColumnType_MySql();
		} else if (databaseType == 2) {
			columnType = getColumnType_MsSql();
		}
		
		return columnType;
	}
	
	private String getColumnType_MySql() {
		
		int len = getLenthForType();
		
		if (len == 0) {
			len = 1;
		}
		
		String columnType = "";
		switch (fieldType) {
		case 1: // datetime
			columnType = "DATETIME DEFAULT NULL";
			break;
		case 2: // varchar
			if (len > 255) {
				columnType = "TEXT DEFAULT NULL";
			} else {
				columnType = "VARCHAR("+len+") DEFAULT NULL";
			}
			break;
		case 3: // int unsigned - with zero fill
			columnType = "INT DEFAULT NULL";
			break;
		case 4: // int
			if (len < 8) {
				columnType = "INT UNSIGNED ZEROFILL DEFAULT " + len;
			} else {
				columnType = "BIGINT UNSIGNED ZEROFILL DEFAULT " + len;
			}
			break;
		case 5: // decimal
			// TODO columnType = "DECIMAL(18, 2) DEFAULT NULL";
			columnType = "VARCHAR(50) DEFAULT NULL";
			break;
		case 6: // empty
			columnType = "VARCHAR("+len+") DEFAULT NULL";
			break;
		case 7: // other
			columnType = "VARCHAR("+len+") DEFAULT NULL";
			break;
		default:
			if (len > 255) {
				columnType = "TEXT DEFAULT NULL";
			} else {
				columnType = "VARCHAR("+len+") DEFAULT NULL";
			}
			break;
		}
		
		return columnType;
	}
	
	private String getColumnType_MsSql() {
		
		int len = getLenthForType();
		
		if (len == 0) {
			len = 1;
		}
		
		String columnType = "";
		switch (fieldType) {
		case 1: // datetime
			columnType = "[DATETIME] NULL";
			break;
		case 2: // varchar
			if (len > 255) {
				columnType = "TEXT DEFAULT NULL";
			} else {
				columnType = "VARCHAR("+len+") DEFAULT NULL";
			}
			break;
		case 3: // int unsigned - with zero fill
			 // TODO ?? for zero fill?
			columnType = "[INT] NULL";
			break;
		case 4: // int
			if (len < 8) {
				columnType = "[INT] NULL";	
			} else {
				columnType = "[BIGINT] NULL";
			}
			break;
		case 5: // decimal
			// TODO - [decimal](18, 0) NULL
			columnType = "[VARCHAR](50) DEFAULT NULL"; 
			break;
		case 6: // empty
			columnType = "[VARCHAR]("+len+") DEFAULT NULL"; // TODO - delete this column later
			break;
		case 7: // other
			columnType = "[VARCHAR]("+len+") DEFAULT NULL"; // TODO - delete this column later
			break;
		default:
			if (len > 255) {
				columnType = "TEXT DEFAULT NULL";
			} else {
				columnType = "VARCHAR("+len+") DEFAULT NULL";
			}
			break;
		}
		
		return columnType;
	}
	
	/**
	 * optimise length of a particular column Type
	 * 
	 * TODO - figure out decimal, watch for before and after . during parse
	 * TODO - do I want to make a percenatage bigger than needed? like make bigger * 10%
	 * 			although, for now I think I will skip this, make it exact, seems to work
	 * 
	 * @return
	 */
	private int getLenthForType() {

		int l = 0;
		
		switch (fieldType) {
		case 1: // datetime
			l = fieldLength;
			break;
		case 2: // varchar
			l = fieldLength;
			break;
		case 3: // int unsigned - with zero fill
			l = fieldLength;
			break;
		case 4: // int
			l = fieldLength;
			break;
		case 5: // decimal
			l = fieldLength;
			break;
		case 6: // empty
			l = fieldLength;
			break;
		case 7: // other
			l = fieldLength;
			break;
			default:
				l = fieldLength;
			break;
		}
		
		return l;
	}
	
	private void setFieldLength(String s) {
		int size = s.length();
		if (size > fieldLength) {
			fieldLength = size;
		}
	}
	
	private boolean isEmpty(String s) {
		boolean b = false;
		if (s.isEmpty()) {
			b = true;
		}
		return b;
	}

	private boolean isText(String s) {
		boolean b = false;
		if (s.matches(".*[a-zA-Z].*")) {
			b = true;
		}
		return b;
	}
	
	private boolean isInt(String s) {
		
		boolean b = false;
		if (s.matches("[0-9]+")) {
			b = true;
		}
		return b;
	}
	
	private boolean isIntZero(String s) {
		boolean b = false;
		if (s.matches("[0-9]+") && s.matches("^0")) {
			b = true;
		}
		return b;
	}
	
	private boolean isDecimal(String s) {
		boolean b = false;
		if (s.matches("^\\d+\\.\\d+|\\.\\d+")) {
			b = true;
		}
		return b;
	}
	
	private boolean isDate(String s) {
		
		s = s.toLowerCase();
		
		if (s.length() == 0) {
			return false;
		}
		
		boolean b = false;
		
		if (s.contains("jan")) {
			b = true;
		} else if (s.contains("feb")) {
			b = true;
		} else if (s.contains("feb")) {
			b = true;
		} else if (s.contains("mar")) {
			b = true;
		} else if (s.contains("apr")) {
			b = true;
		} else if (s.contains("may")) {
			b = true;
		} else if (s.contains("jun")) {
			b = true;
		} else if (s.contains("jul")) {
			b = true;
		} else if (s.contains("aug")) {
			b = true;
		} else if (s.contains("sep")) {
			b = true;
		} else if (s.contains("oct")) {
			b = true;
		} else if (s.contains("nov")) {
			b = true;
		} else if (s.contains("dec")) {
			b = true;
		} 
		
		// TODO - proof this later
		if (s.matches("[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4}.*")) {
			b = true;
		}
		
		return b;
	}
	


}
