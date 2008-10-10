package com.tribling.csv2sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import com.tribling.csv2sql.data.DestinationData;
import com.tribling.csv2sql.data.MatchFieldData;
import com.tribling.csv2sql.data.SourceData;

/**
 * sql processing
 * 
 * @author branflake2267
 *
 */
public class SQLProcessing {

	// [MySQL=1|MsSql=2]
	private int databaseType = 1;
	
	private String database;
	private String table;
	private String username;
	private String password;
	private String host;
	private String port;

	private Connection conn;

	// match source to destination field
	private MatchFieldData[] matchFields;
	
	/**
	 * constructor
	 */
	public SQLProcessing() {
	}
	
	/**
	 * set the destination data
	 * 
	 * @param destinationData
	 * @throws Exception
	 */
	protected void setDestinationData(DestinationData destinationData) throws Exception {
		
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
			System.err.println("ERROR: No username: Whats the sql database username?");
			throw new Exception();
		}
		
		if (destinationData.password.length() > 0) {
			this.password = destinationData.password;
		} else {
			System.err.println("ERROR: No password: Whats the sql database password?");
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
			System.err.println("ERROR: No port: what doorway is the sql server behind? ie. [3306|1433]");
			throw new Exception();
		}
	}
	
	protected void setMatchFields(MatchFieldData[] matchFields) {
		this.matchFields = matchFields;
	}
	
	/**
	 * open the sql connection to work with
	 * 
	 * @throws Exception 
	 */
	protected void openConnection() {
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
			query = "SHOW TABLES FROM `" + database + "` LIKE `" + table + "`;";
		} else if (databaseType == 2) {
			query = "SELECT NAME FROM [" + database + "].SYSOBJECTS WHERE TYPE='U' AND NAME='" + table + "';";
		}
		
		boolean rtn = getBooleanQuery(query);
		
		return rtn;
	}
	
	/**
	 * create table
	 */
	private void createTable() {
		
		boolean doesExist = isTableExist();
		if(doesExist == true) {
			return;
		}
		
		table = fixName(table);
		
		String query = "";
		if (databaseType == 1) {
			query = "CREATE TABLE `" + database + "`.`" + table + "` (" +
					"`ImportID` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ImportID`) " +
					") ENGINE = MyISAM;";
		} else if (databaseType == 2) {
			query = "CREATE TABLE [" + database + "].[" + table + "] ( ImportID int identity primary key );";
		}
		
		setUpdateQuery(query);
	}
	
	/**
	 * sql drop table
	 */
	private void dropTable() {
		String query = "";
		if (databaseType == 1) {
			query = "DROP TABLE IF EXISTS `" + database + "`.`" + table + "`;";
		} else if (databaseType == 2) {
			if (isTableExist()) {
				query = "DROP TABLE [" + database + "].[" + table + "];";
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
			query = "SELECT * FROM [" + database + "].INFORMATION_SCHEMA.Columns WHERE TABLE_NAME='[" + table + "]';";
		}
		
		boolean rtn = getBooleanQuery(query);
		
		return rtn;
	}
	
	
	protected String[] getColumns() {
		String query = "";
		if (databaseType == 1) {
			query = "SHOW COLUMNS FROM `" + table + "` FROM `" + database + "`;";
		} else if (databaseType == 2) {
			query = "SELECT * FROM [" + database + "].INFORMATION_SCHEMA.Columns;";
		}

		String[] columns = null;
		try {
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			
			int rcount = getResultSetSize(result);
			columns = new String[rcount];
			
			int i=0;
			while(result.next()) {
				columns[i] = result.getString(1);
				i++;
			}
			result.close();
		} catch (Exception e) {
			System.err.println("Mysql Statement Error:" + query);
			e.printStackTrace();
		}
		
		return columns;
	}
	
	protected void createColumns(String[] columns) {
		
		String type = "TEXT";
		
		for (int i=0; i < columns.length; i++) {
			createColumn(columns[i], type);
		}
	}
	
	/**
	 * create a column
	 * @param column
	 * @param type
	 */
	private void createColumn(String column, String type) {
		
		column = replaceToMatchingColumn(column);
		
		boolean exist = isColumnExist(column);
		if(exist == true) {
			return;
		}
		
		column = fixName(column);
		
		if (type == null) {
			type = "TEXT";
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "ALTER `" + database + "`.`" + table + "` ADD '" + column + "' " + type + ";"; // TODO -> type = VARCHAR(60)
		} else if (databaseType == 2) {
			query = "ALTER TABLE [" + database + "].[" + table + "] ADD '" + column + "' " + type + ";";
		}
		
		setUpdateQuery(query);
	}
	
	private String replaceToMatchingColumn(String column) {
		
		//TODO - search array for source column ???????????????????????????????
		int index = Arrays.binarySearch(matchFields, column);
		
		if (index > 0) {
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
	private void setUpdateQuery(String query) {
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
	
	/**
	 * fix the name to be sql friendly
	 * @param s
	 * @return
	 */
	private String fixName(String s) {
		
		s = s.replace("[\"\r\n\t]", "");
		s = s.replace("!", "");
		s = s.replace("@", "");
		s = s.replace("#", "");
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
		s = s.replace("\\.", "");
		s = s.replace(",", "");
		s = s.replace("\\.", "");
		s = s.replace("<", "");
		s = s.replace(">", "");
		s = s.replace("\\?", "");
		s = s.replace("&", "");
		s = s.replace("\\/", "");
		s = s.replace("%", "");
		s = s.replace(" ", "_");
			
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
	public void addData(String[] columns, String[] values) {
		

		
	}
	
	private void updateData() {

		String query = "";
		if (databaseType == 1) {
			query = "";
		} else if (databaseType == 2) {
			query = "";
		}

	}
}









