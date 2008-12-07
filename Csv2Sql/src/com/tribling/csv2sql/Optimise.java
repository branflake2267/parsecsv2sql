package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.tribling.csv2sql.data.ColumnData;

public class Optimise extends SQLProcessing {
	
	private int fieldType = 0;
	private int fieldLength = 0;

	/**
	 * constructor
	 */
	public Optimise() {
	}
	
	protected void runOptimise(ColumnData[] columns) {

		if (dd.optimise == false) {
			System.out.println("skipping optimising: destinationData.optimise = false");
			return;
		}
		
		// loop through each column
		loopThroughColumns(columns);
		
		System.out.println("Done Optimising");
	}

	public void runOptimise() {
		
		if (dd.optimise == false) {
			System.out.println("skipping optimising: destinationData.optimise = false");
			return;
		}
		
		openConnection();
	
		// do this first, b/c it will cause problems after getting columns
		deleteEmptyColumns();
		
		ColumnData[] columns = getColumns();
		
		columns = fixColumns(columns);
	
		// loop through each column
		loopThroughColumns(columns);
	
		closeConnection();
	}
	
	/**
	 * resize a column
	 * 
	 * @param column
	 * @param columnType
	 * @param length
	 */
	public String resizeColumn(String column, String columnType, int length) {
		this.fieldLength = length;
		this.fieldType = getFieldType(columnType);
		
		String type = "";
		if (databaseType == 1) {
			type = getColumnType_MySql();
		} else if (databaseType == 2) {
			type = getColumnType_MsSql();
		}
		
		alterColumn(column, type);
		return type;
	}
	
	private void loopThroughColumns(ColumnData[] columns) {
		
		int i2 = columns.length - 1;
		for(int i=0; i < columns.length; i++) {
			
			// reset
			fieldType = 0;
			fieldLength = 0;
		
			// console
			System.out.println(i2 + ". Analyzing column: " + columns[i].column);
			
			// analyze column
			analyzeColumn(columns[i].column);
			
			// get type that was determined by analyzeColumn
			String columnType = getColumnType();
			
			// alter column
			// TODO - do we need to alter it, skip if already perfect
			alterColumn(columns[i].column, columnType);
			
			i2--;
		}
		
	}

	private void alterColumn(String column, String columnType) {
		
		if (column.equals("ImportID") | 
				column.equals("DateCreated") | 
				column.equals("DateUpdated")) {
			return;
		}
		
		String modifyColumn = "";
		String alterQuery = "";
		if (databaseType == 1) {
			modifyColumn = "`" + column + "` " + columnType;
			alterQuery = "ALTER TABLE `" + dd.database + "`.`" + dd.table + "` MODIFY COLUMN " + modifyColumn;
		} else if (databaseType == 2) {
			modifyColumn = "[" + column + "] " + columnType;
			alterQuery = "ALTER TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + " ALTER COLUMN " + modifyColumn;
		}
		
		updateSql(alterQuery);
		
	}
	
	private String getLimitQuery() {
		
		// when this is set to 0 sample all
		if (dd.optimiseRecordsToExamine == 0) {
			return "";
		}

		String s = "";
		if (databaseType == 1) {
			s = " LIMIT 0," + dd.optimiseRecordsToExamine + " ";
		} else if (databaseType == 2) {
			s = " TOP " + dd.optimiseRecordsToExamine + " ";
		}
		return s;
	}
	
	/**
	 * analyze a column for its type and length
	 * 
	 * @param column
	 */
	private void analyzeColumn(String column) {
		
		// when a selection is noted, sample randomly
		String random = "";
		if (dd.optimiseRecordsToExamine == 0) {
			if (databaseType == 1) {
				random = "ORDER BY RAND()";
			} else if(databaseType == 2) {
				random = "ORDER BY NEWID()";
			}
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "SELECT " + column + " " +
					"FROM " + dd.database + "." + dd.table + " " + random + " " + getLimitQuery() + ";";
		} else if (databaseType == 2) {
			query = "SELECT " + getLimitQuery() + " " + column + " " +
					"FROM " + dd.database + "." + dd.tableSchema + "." + dd.table + " "+random+";";
		}
		
		System.out.println("Analyzing Column For Type: " + column + " query: " + query );
		
		try {
			Connection conn = getConnection();
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			while(result.next()) {
				String s = result.getString(1);
				examineField(s);
			}
			select.close();
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
		if (dd.optimiseTextOnly == true) {
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
	
	private int getFieldType(String columnType) {
		
		String type = columnType.toLowerCase();
		
		int fieldType = 0;
		if (type.contains("text")) {
			fieldType = 2;
		} else if (type.contains("date")) {
			fieldType = 1;
		} else if (type.contains("varchar")) {
			fieldType = 2;
		} else if (type.contains("int")) {
			fieldType = 3;
		} else if (type.contains("decimal")) {
			fieldType = 5;
		} else if (type.length() == 0) {
			fieldType = 6;
		}
		
		return fieldType;
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
				columnType = "TEXT NULL";
			} else {
				columnType = "VARCHAR("+len+") NULL";
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
			columnType = "[VARCHAR](50) NULL"; 
			break;
		case 6: // empty
			columnType = "[VARCHAR]("+len+") NULL"; // TODO - delete this column later
			break;
		case 7: // other
			columnType = "[VARCHAR]("+len+") NULL"; // TODO - delete this column later
			break;
		default:
			if (len > 255) {
				columnType = "TEXT DEFAULT NULL";
			} else {
				columnType = "VARCHAR("+len+") NULL";
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
