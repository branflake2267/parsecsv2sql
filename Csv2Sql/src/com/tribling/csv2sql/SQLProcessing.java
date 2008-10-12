package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.lang.StringEscapeUtils;

import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SortSourceField;


/**
 * sql processing
 * 
 * @author branflake2267
 *
 */
public class SQLProcessing {

	// [MySQL=1|MsSql=2]
	protected int databaseType = 1;
	
	protected String database;
	protected String table;
	
	private String username;
	private String password;
	
	private String host;
	private String port;

	protected Connection conn;

	// match source to destination field
	private MatchFieldData[] matchFields;

	// if a user wants to drop a table on insert
	private boolean dropTable = true;
	
	// can turn it off for files greater than 0
	private boolean dropTableOff = false;
	
	protected String tableSchema;
	
	// track where at in the importing
	private int index = 0;
	private int indexFile = 0;

	// optimise on or off
	protected boolean optimise = false;

	// optmise examine how many records
	protected int optimiseRecordsToExamine = 1000;
	
	// only optimise (drill down alter) to varchar/text columns
	// this has to be done first anyway, so we can alter varchar->date, varchar->int...
	protected boolean optimiseTextOnly = true;

	// don't delete empty columns (delete them if set to true)
	private boolean deleteEmptyColumns = false;

	/**
	 * constructor
	 */
	public SQLProcessing() {
	}
	
	public void dropTableOff() {
		this.dropTableOff = true;
	}
	
	/**
	 * set the destination data
	 * 
	 * @param destinationData
	 * @throws Exception
	 */
	public void setDestinationData(DestinationData destinationData) throws Exception {
		
		if (destinationData.databaseType.equals("MySql")) {
			this.databaseType = 1;
		} else if (destinationData.databaseType.equals("MsSql")) {
			this.databaseType = 2;
		} else {
			System.err.println("ERROR: No DatabaseTye: [MySql|MsSql]");
			throw new Exception();
		}
		
		if (destinationData.database.length() > 0) {
			this.database = destinationData.database;
		} else {
			System.err.println("ERROR: No Database: Whats the database name?");
			throw new Exception();
		}
		
		if (destinationData.username.length() > 0) {
			this.username = destinationData.username;
		} else {
			System.err.println("ERROR: No username: What is the sql database username?");
			throw new Exception();
		}
		
		if (destinationData.password.length() > 0) {
			this.password = destinationData.password;
		} else {
			System.err.println("ERROR: No password: What is the sql database password?");
			throw new Exception();
		}
		
		if (destinationData.host.length() > 0) {
			this.host = destinationData.host;
		} else {
			System.err.println("ERROR: No host: Where is the server located? ie. [IpAddress|host.domain.tld]");
			throw new Exception();
		}
		
		if (destinationData.port.length() > 0) {
			this.port = destinationData.port;
		} else {
			System.err.println("ERROR: No port: What doorway is the sql server behind? ie. [3306|1433]");
			throw new Exception();
		}
		
		if (destinationData.table.length() > 0) {
			this.table = destinationData.table;
		} else {
			System.err.println("ERROR: No destination table: What table do you want to import this data to?");
			throw new Exception();
		}
		
		this.dropTable = destinationData.dropTable;
		this.table = destinationData.table;
		this.tableSchema = destinationData.tableSchema;
		this.optimise = destinationData.optimise;
		this.optimiseRecordsToExamine = destinationData.optimiseRecordsToExamine;
		this.deleteEmptyColumns = destinationData.deleteEmptyColumns;
		this.optimiseTextOnly = destinationData.optimiseTextOnly;
		
		// open a sql connection to work with
		openConnection();
	}
	
	protected void setMatchFields(MatchFieldData[] matchFields) {
		this.matchFields = matchFields;
	}
	
	/**
	 * open the sql connection to work with
	 * 
	 * @throws Exception 
	 */
	private void openConnection() {
		if (databaseType == 1) {
			conn = getConn_MySql();
		} else if (databaseType == 2) {
			conn = getConn_MsSql();
		}
	}
	
	/**
	 * does table exist?
	 * 
	 * @return
	 */
	private boolean isTableExist() {
		
		String query = "";
		if (databaseType == 1) {
			query = "SHOW TABLES FROM `" + database + "` LIKE '" + table + "';";
		} else if (databaseType == 2) {
			query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
					"WHERE TABLE_CATALOG = '" + database + "' AND " +
					"TABLE_SCHEMA = '" + tableSchema + "' AND " +
					"TABLE_NAME = '" + table + "'";
		}
		
		boolean rtn = getBooleanQuery(query);
		
		return rtn;
	}
	
	/**
	 * create table
	 * 
	 */
	public void createTable() {
		
		boolean doesExist = isTableExist();
		if(doesExist == true) {
			
			if (dropTable == true && dropTableOff == false) {
				dropTable();
			} else if (dropTable == false && doesExist == false) {
				return;
			} else if (dropTableOff == true && doesExist == true) {
				return;
			}
			
			
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "CREATE TABLE `" + database + "`.`" + table + "` (" +
					"`ImportID` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ImportID`) " +
					") ENGINE = MyISAM;";
		} else if (databaseType == 2) {
			query = "CREATE TABLE " + database + "." + tableSchema + "." + table + " " +
					"( [ImportID] [int] IDENTITY(1,1) NOT NULL);";
		}
		
		setUpdateQuery(query);
	}
	
	/**
	 * sql drop table
	 */
	private void dropTable() {
		
		if (dropTableOff == true) {
			return;
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "DROP TABLE IF EXISTS `" + database + "`.`" + table + "`;";
		} else if (databaseType == 2) {
			if (isTableExist()) {
				query = "DROP TABLE " + database + "." + tableSchema + "." + table + ";";
			}
		}
		
		setUpdateQuery(query);
	}

	/**
	 * does column exist?
	 * 
	 * @param column
	 * @return
	 */
	private boolean isColumnExist(String column) {
		
		String query = "";
		if (databaseType == 1) {
			query = "SHOW COLUMNS FROM `" + table + "` FROM `" + database + "` LIKE '" + column + "';";
		} else if (databaseType == 2) {
			query = "SELECT COLUMN_NAME FROM " + database + ".INFORMATION_SCHEMA.Columns " +
					"WHERE TABLE_SCHEMA='" + tableSchema + "' AND " +
					"TABLE_NAME='" + table + "' AND " +
					"COLUMN_NAME = '" + column + "';";
		}
		
		boolean rtn = getBooleanQuery(query);
		
		return rtn;
	}
	
	
	protected String[] getColumns() {
		String query = "";
		if (databaseType == 1) {
			query = "SHOW COLUMNS FROM `" + table + "` FROM `" + database + "`;";
		} else if (databaseType == 2) {
			query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.Columns " +
					"WHERE TABLE_NAME ='" + table + "' AND " +
					"TABLE_SCHEMA='" + tableSchema + "' AND TABLE_CATALOG='" + database + "'";
		}

		ArrayList<String> c = new ArrayList<String>();
		try {
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			while(result.next()) {
				c.add(result.getString(1));
			}
			result.close();
		} catch (Exception e) {
			System.err.println("Mysql Statement Error:" + query);
			e.printStackTrace();
		}
		
		// have to use array list due to not knowing result size on ms jdbc
		String[] columns = new String[c.size()];
		for(int i=0; i < c.size(); i++) {
			columns[i] = (String) c.get(i);
		}
		
		return columns;
	}
	
	/**
	 * create columns
	 * 
	 * TODO - examine rows in csv, maybe need to do TEXT as default, but date doesn't cast(alter) from text in mssql
	 * 
	 * @param columns
	 * @return
	 */
	protected String[] createColumns(String[] columns) {
		
		columns = fixColumns(columns);
		
		// Two reasons to start with text.
		// 1. fields can have length > 255
		// 2. 65535 length limit of sql insert
		// NOTE: have to alter to varchar/text first then drill down further after that
		String type = "TEXT DEFAULT NULL";
		
		for (int i=0; i < columns.length; i++) {
			createColumn(columns[i], type);
		}
		
		return columns;
	}
	
	protected String[] fixColumns(String[] columns) {
		
		ArrayList<String> aColumns = new ArrayList<String>();
		for(int i=0; i < columns.length; i++) {
			if (columns[i] == "") {
				columns[i] = "c" + i;
			}
			aColumns.add(columns[i].trim());
		}
		
		columns = new String[aColumns.size()];
		
		for (int i=0; i < aColumns.size(); i++) {
			String c = aColumns.get(i);
			columns[i] = replaceToMatchingColumn(c);
			columns[i] = fixName(columns[i]);
		}
		
		return columns;
	}
	
	/**
	 * create a column
	 * @param column
	 * @param type
	 */
	private void createColumn(String column, String type) {
		
		boolean exist = isColumnExist(column);
		if(exist == true) {
			return;
		}

		if (type == null) {
			type = "TEXT";
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "ALTER TABLE `" + database + "`.`" + table + "` ADD COLUMN `" + column + "`  " + type + ";";
		} else if (databaseType == 2) {
			query = "ALTER TABLE " + database + "." + tableSchema + "." + table + " ADD [" + column + "] " + type + ";";
		}
		
		setUpdateQuery(query);
	}
	
	private String replaceToMatchingColumn(String column) {
		
		if (matchFields == null) {
			//System.out.println("No matching fields entered: skipping (replaceToMatchingColumn)");
			return column;
		}
		
		System.out.println("Match Column: " + column);
		
		Comparator<MatchFieldData> searchByComparator = new SortSourceField();
		
		MatchFieldData searchFor = new MatchFieldData();
		searchFor.sourceField = column;
		
		int index = Arrays.binarySearch(matchFields, searchFor, searchByComparator);
		
		if (index >= 0) {
			column = matchFields[index].desinationField;
		}
		
		return column;
	}
	
	private Connection getConn_MySql() {

		String url = "jdbc:mysql://"+host+":"+port+"/";
		String driver = "com.mysql.jdbc.Driver";
		System.out.println("getConn_MySql: url:" + url + " user: " + username + " driver: " + driver);
		
		Connection conn = null;
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url + database, username, password);
		} catch (Exception e) {
			System.err.println("MySql Connection Error:");
			e.printStackTrace();
		}

		return conn;
	}
	
	/**
	 * get ms sql connection
	 * 
	 *  // Note: this class name changes for ms sql server 2000 thats it
     *  // It has to match the JDBC library that goes with ms sql 2000
	 * 
	 * @return
	 */
	private Connection getConn_MsSql() {

		String url = "jdbc:sqlserver://" + host + ";user=" + username + ";password=" + password + ";databaseName=" + database + ";";
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		System.out.println("getConn_MsSql: url:" + url + " user: " + username + " driver: " + driver);
		
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url);
		} catch (Exception e) {
			System.err.println("MsSql Connection Error: " + e.getMessage());
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * close sql connection
	 * @param conn
	 */
	protected void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.err.println("MsSql Connection Closing Error: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * update query
	 * 
	 * @param query
	 */
	public void setUpdateQuery(String query) {
		
		System.out.println("f:" + indexFile + ": row:" + index + ". " + query);
		
		try {
            Statement update = conn.createStatement();
            update.executeUpdate(query);
        } catch(Exception e) { 
        	System.err.println("Mysql Statement Error: " + query);
        	e.printStackTrace();
        }
	}
	
	/**
	 * does a value exist? return boolean
	 * 
	 * @param query
	 * @return
	 */
	private boolean getBooleanQuery(String query) {

		String value = null;
		try {
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			while(result.next()) {
				value = result.getString(1);
			}
			result.close();
		} catch (Exception e) {
			System.err.println("Mysql Statement Error:" + query);
			e.printStackTrace();
		}
		
		boolean rtn = false;
		if (value != null) {
			rtn = true;
		}
		
		return rtn;
	}
	
	private int getQueryInt(String query) {

		int i = 0;
		try {
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			while(result.next()) {
				i = result.getInt(1);
			}
			result.close();
		} catch (Exception e) {
			System.err.println("Mysql Statement Error:" + query);
			e.printStackTrace();
		}
		
		return i;
	}
	
	/**
	 * fix the name to be sql friendly
	 * @param s
	 * @return
	 */
	private String fixName(String s) {
		
		s = s.replace("[\"\r\n\t]", "");
		s = s.replace("!", "");
		s = s.replace("@", "");
		s = s.replace("#", "_Num");
		s = s.replace("$", "");
		s = s.replace("^", "");
		s = s.replace("\\*", "");
		s = s.replace("\\", "");
		s = s.replace("\\+", "");
		s = s.replace("=", "");
		s = s.replace("~", "");
		s = s.replace("`", "");
		s = s.replace("\\{", "");
		s = s.replace("\\}", "");
		s = s.replace("\\[", "");
		s = s.replace("\\]", "");
		s = s.replace("\\|", "");
		s = s.replace(".", "_");
		s = s.replace(",", "");
		s = s.replace("\\.", "");
		s = s.replace("<", "");
		s = s.replace(">", "");
		s = s.replace("?", "");
		s = s.replace("&", "");
		s = s.replace("/", "");
		s = s.replace("%", "_per");
		s = s.replace(" ", "_");
		s = s.replace("(", "");
		s = s.replace(")", "");
	
		return s;
	}
	
	/**
	 * get row count
	 * 
	 * @param result
	 * @return
	 */
	protected static int getResultSetSize(ResultSet result) {
		int size = -1;

		try {
			result.last();
			size = result.getRow();
			result.beforeFirst();
		} catch (SQLException e) {
			return size;
		}

		return size;
	}
	
	/**
	 * get row data into array
	 * 
	 * @param columns
	 * @param result
	 * @return
	 */
	protected String[] getRowData(String[] columns, ResultSet result) {
		
		String[] rtn = new String[columns.length];
		for (int i=0; i < columns.length; i++) {
			try {
				rtn[i] = result.getString(i+1);
			} catch (SQLException e) {
				System.err.println("Error in getRowData"); 
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * input array to a csv string
	 * 
	 * @param inputArray
	 * @return
	 */
	private String implodeArray(String[] inputArray) {

		String s;
		if (inputArray.length == 0) {
			s = "";
		} else {
			StringBuffer sb = new StringBuffer();
			sb.append(inputArray[0]);
			for (int i = 1; i < inputArray.length; i++) {
				sb.append(",");
				sb.append(inputArray[i]);
			}
			s = sb.toString();
		}

		return s;
	}
	
	/**
	 * add data to table
	 * 
	 * @param columns
	 * @param values
	 */
	public void addData(int indexFile, int index, String[] columns, String[] values) {
		this.index = index;
		this.indexFile = indexFile;
		
		String query = null;
		if (databaseType == 1) {
			
			// TODO - is this data in database already?
			
			query = getQuery_Insert_MySql(columns, values);
			
		} else if (databaseType == 2) {
			
			// TODO _ is this data int he database already?
			
			query = getQuery_Insert_MsSql(columns, values);
		}
		
		if (query != null) {
			setUpdateQuery(query);
		}

	}
	
	private String getQuery_Insert_MsSql(String[] columns, String[] values) {
		
		if (columns.length != values.length) {
			//System.out.print("Colunns:" + columns.length + " != Values:" + values.length + " ");
			
			//if (values.length < columns.length) {
				//return null;
			//}
		}
		
		String cs = "";
		String vs = "";
		for(int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c =  columns[i];
			try {
				v = values[i];
			} catch (Exception e1) {
				v = "";
			}	
			
			// TODO ms sql truncate error? change to alter to text
			if (v != null) {
				int len = v.length();
				if (len > 255) {
					v = v.substring(0,255);
				}
			}
			
			cs += "[" + c + "]";
			vs += "'" + escapeForSql(values[i]) + "'";

			if(i < columns.length-1) {
				cs += ", ";
				vs += ", "; 
			}
		}
		
		String s = "INSERT INTO " + database + "." + tableSchema + "." + table + " (" + cs + ") VALUES (" + vs + ");";
		
		return s;
	}
	
	private String getQuery_Insert_MySql(String[] columns, String[] values) {
		
		if (columns.length != values.length) {
			//System.out.print("Colunns:" + columns.length + " != Values:" + values.length + " ");
			
			//if (values.length < columns.length) {
				//return null;
			//}
		}
		
		String q = "";
		for (int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c = columns[i];
			
			// when there a no values for this columns position
			try {
				v = values[i];
			} catch (Exception e) {
				v = "";
			}
			
			// TODO truncate error? change to alter to text
			if (v != null) {
				int len = v.length();
				if (len > 255) {
					v = v.substring(0,255);
				}
			}
			
			v = escapeForSql(v);
			
			q += "`" + c + "`='" + v + "'";

			if(i < columns.length-1) {
				q += ", ";
			}
		}
		
		String s = "INSERT INTO `" + database + "`.`" + table + "` SET "+q+";";
		
		return s;
	}
	
	// TODO
	private String getQuery_Update_MsSql(String[] columns, String[] values) {
		
		if (columns.length != values.length) {
			System.out.println("Error (Update) in columns lenth and values length");
		}
		

		String q = "";
		for (int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c = columns[i].trim();
			
			try {
				v = values[i];
			} catch (Exception e) {
				v = "";
			}
			
			v = escapeForSql(v);
			
			q += "[" + c + "]='" + v + "'";
			
			if(i < columns.length-1) {
				q += ",";
			}
		}
		
		String s = "UPDATE " + database + "." + tableSchema + "." + table + " "+q+" WHERE ";
		
		return s;
	}
	
	// TODO
	private String getQuery_Update_MySql(String[] columns, String[] values) {
		
		
		String s = "";
		return s;
	}
	
	// TODO
	public int doesRowExistAlready_MsSql() {
		
		String query = "SELECT ImportId FROM " + database + "." + tableSchema + "." + table + " WHERE ";
		
		return 0;
	}
	
	// TODO
	public int doesRowExistAlready_MySql() {
		
		String query = "SELECT ImportId FROM `" + database + "`.`" + table + "` WHERE ";
		
		return 0;
	}
			
	/**
	 * escape string to db
	 * 
	 * remove harmfull db content
	 * remove harmfull tags
	 *
	 * @param s
	 * @return
	 */
	protected static String escapeForSql(String s) {
	        
	        s = StringEscapeUtils.escapeSql(s);
	        
	        //escape utils returns null if null
	        if (s == null) {
	                s = "";
	        }
	        
	        s = s.replace("\\", "\\\\");
	        
	        s = s.trim();
	        
	        return s;
	}
	
	/**
	 * delete empty columns - any column with no values (all null | '')
	 */
	public void deleteEmptyColumns() {
		
		if (deleteEmptyColumns == false) {
			System.out.println("Skipping deleting empty Columns: destinationData.deleteEmptyColumns=false");
		}
		
		System.out.println("Going to delete empty columns");
		
		String[] columns = getColumns();
		
		for(int i=0; i < columns.length; i++) {
			System.out.print(".");
			if (getColumnHasStuff(columns[i]) == false) {
				deleteColumn(columns[i]);
			}
		}
	}
	
	private boolean getColumnHasStuff(String column) {
		
		String query = "";
		if (databaseType == 1) {
			query = "SELECT COUNT(`" + column + "`) AS Total FROM `" + database + "`.`" + table + "` " +
					"WHERE (`" + column + "` != '');";
		} else if (databaseType == 2) {
			query = "SELECT COUNT([" + column + "]) AS Total FROM " + database + "." + tableSchema + "." + table + " " +
					"WHERE  ([" + column + "] != '');";
		}
		
		int i = getQueryInt(query);
		
		boolean b = true;
		if (i == 0) {
			b = false;
		}
		
		return b;
	}
	
	private void deleteColumn(String column) {
		
		String query = "";
		if (databaseType == 1) {
			query = "ALTER TABLE `" + database + "`.`" + table + "` DROP COLUMN `"+column+"`;";
		} else if (databaseType == 2) {
			query = "ALTER TABLE " + database + "." + tableSchema + "." + table + " DROP COLUMN " + column + ";";
		}
		
		setUpdateQuery(query);
	}
}









