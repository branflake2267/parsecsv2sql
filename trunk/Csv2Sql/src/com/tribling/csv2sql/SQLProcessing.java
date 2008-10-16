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
import com.tribling.csv2sql.data.IdentityData;
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

	int connLoadBalance = 0;
	protected Connection conn1;
	protected Connection conn2;

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

	private boolean checkForExistingRecordsAndUpdate = false;
	
	// identity columns to index and insert/update
	private MatchFieldData[] identityColumns = null;

	

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
		this.identityColumns = destinationData.identityColumns;
		this.checkForExistingRecordsAndUpdate = destinationData.checkForExistingRecordsAndUpdate;
		
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
			conn1 = getConn_MySql();
		} else if (databaseType == 2) {
			conn1 = getConn_MsSql();
		}
		
		if (databaseType == 1) {
			conn2 = getConn_MySql();
		} else if (databaseType == 2) {
			conn2 = getConn_MsSql();
		}
	}
	
	public void closeConnection() {
		try {
			conn1.close();
		} catch (Exception e) {
		}
		try {
			conn2.close();
		} catch (SQLException e) {
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
					"`ImportID` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ImportID`) " +
					") ENGINE = MyISAM;";
		} else if (databaseType == 2) {
			query = "CREATE TABLE " + database + "." + tableSchema + "." + table + " " +
					"( [ImportID] [INT] IDENTITY(1,1) NOT NULL);";
		}
		
		setUpdateQuery(query);
		
		// track changes by date
		String column = "DateCreated";
		String type = "";
		if (databaseType == 1) {
			type = "DATETIME DEFAULT NULL";
		} else if (databaseType == 2) {
			type = "DATETIME NULL";
		}
		createColumn(column, type);
		
		if (checkForExistingRecordsAndUpdate == true) {
			
			// add DateUpdated column to track updates
			column = "DateUpdated";
			type = "";
			if (databaseType == 1) {
				type = "DATETIME DEFAULT NULL";
			} else if (databaseType == 2) {
				type = "DATETIME NULL";
			}
			createColumn(column, type);
		} 
			
		

		// TODO create indexes of identity columns
		
		
		
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
			Connection conn = getConnection();
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
		String type = "";
		if (databaseType == 1) {
			type = "TEXT DEFAULT NULL";
		} else if (databaseType == 2) {
			type = "VARCHAR(255) NULL"; // TODO -aggregate functions don't work with this. Need to alter to text with lenths greather than text
		}
		
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
			if (databaseType == 1) {
				type = "TEXT";
			} else if (databaseType == 2) {
				// For some reason I don't undertand why I cant alter from text->varchar dump, mysql does it fine
				type = "VARHCAR(255)"; //
			}
			
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
		
		//System.out.println("Match Column: " + column);
		
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
	 * go back in forth over a connection to load balance over more than one cpu
	 * @return
	 */
	protected Connection getConnection() {
		
		Connection c = null;
		if (connLoadBalance == 0) {
			connLoadBalance = 1;
			c = conn1;
		} else {
			connLoadBalance = 0;
			c = conn2;
		}
		
		return c;
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
			Connection conn = getConnection();
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
			Connection conn = getConnection();
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
			Connection conn = getConnection();
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
	
	private int getQueryIdent(String query) {
		int i = 0;
		try {
			Connection conn = getConnection();
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
	 * add data to table
	 * 
	 * @param columns
	 * @param values
	 */
	public void addData(int indexFile, int index, String[] columns, String[] values) {
		this.index = index;
		this.indexFile = indexFile;
		
		// does record already exist?
		int id = 0;
		if (checkForExistingRecordsAndUpdate == true) {
			id = doesIdentityExist(columns, values);
		}
		
		String query = null;
		if (databaseType == 1) {
			
			if (id > 0) {
				query = getQuery_Update_MySql(columns, values, id);
			} else {
				query = getQuery_Insert_MySql(columns, values);
			}
			
		} else if (databaseType == 2) {
			
			if (id > 0) {
				query = getQuery_Update_MsSql(columns, values, id);
			} else {
				query = getQuery_Insert_MsSql(columns, values);
			}
			
		}
		
		if (query != null) {
			setUpdateQuery(query);
		}

	}
	
	private String getQuery_Insert_MsSql(String[] columns, String[] values) {
		
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

			cs += "[" + c + "]";
			try {
				vs += "'" + escapeForSql(values[i]) + "'";
			} catch (Exception e) {
				vs += "''";
			}
			
			if(i < columns.length-1) {
				cs += ", ";
				vs += ", "; 
			}
		}
		
		String s = "INSERT INTO " + database + "." + tableSchema + "." + table + " (DateCreated, " + cs + ") " +
				"VALUES (GETDATE(), " + vs + ");";
		
		return s;
	}
	
	private String getQuery_Insert_MySql(String[] columns, String[] values) {
		
		String q = "";
		for (int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c = columns[i];
			
			try {
				v = values[i];
			} catch (Exception e) {
				v = "";
			}
			
			v = escapeForSql(v);
			
			q += "`" + c + "`='" + v + "'";

			if(i < columns.length-1) {
				q += ", ";
			}
		}
		
		String s = "INSERT INTO `" + database + "`.`" + table + "` " +
				"SET DateCreated=NOW(), "+q+";";
		
		return s;
	}
	
	
	
	
	
	
	
	
	
	private void createIndexes() {
		
		if (identityColumns == null) {
			return;
		}
		
		for(int i=0; i < identityColumns.length; i++) {
			createIndex(i, identityColumns[i].desinationField);
		}
		
	}
	
	private void createIndex(int index, String column) {
		
		String indexName = "index" + index;
		
		String query = "";
		if (databaseType == 1) { // ADD INDEX `"+column+"`(`"+indexName+"`),
			query = "ALTER TABLE `" + database + "`.`" + table + "` ADD INDEX `"+column+"`(`"+indexName+"`);";
		} else if (databaseType == 2) {
			// TODO // CREATE INDEX customerid ON klump (CustomerID)
			query = "ALTER TABLE " + database + "." + tableSchema + "." + table + " ADD INDEX [" + column + "](["+indexName+"]) ;";
		}
		
		setUpdateQuery(query);
	}
	
	private String getIdentiesWhereQuery(String[] columns, String[] values) {
		
		IdentityData[] identityData = findIdentityData(columns, values);

		String q = "";
		for (int i=0; i < identityData.length; i++) {
			
			String c = identityData[i].column;
			String v = escapeForSql(identityData[i].value);
			
			if (databaseType == 1) {
				q += "(`"+c+"`='"+v+"')";
			} else if (databaseType == 2) {
				q += "(["+c+"]='"+v+"');";
			}
			
			if (i < identityData.length-1) {
				q += " AND ";
			}
			
		}
		
		return q;
	}
	
	/**
	 * find identity columns
	 * 
	 * @param columns
	 * @return
	 */
	private IdentityData[] findIdentityData(String[] columns, String[] values) {
		
		IdentityData[] ident = new IdentityData[identityColumns.length];
		for (int i=0; i < identityColumns.length; i++) {
			String identColumnName = identityColumns[i].desinationField;

			int index = searchArray(columns, identColumnName);
			
			ident[i] = new IdentityData();
			ident[i].column = identityColumns[i].desinationField;
			try {
				ident[i].value = values[index];
			} catch (Exception e) {
				ident[i].value = "NO IDENTITY VALUE";
			}
		}
		
		return ident;
	}
	
	private int searchArray(String[] a, String m) {
		
		int index = 0;
		for (int i=0; i < a.length; i++) {
			
			String aa = a[i];
			
			if (aa.equals(m)) {
				index = i;
				break;
			}
		}
		return index;
	}
	

	public int doesIdentityExist(String[] columns, String[] values) {
		
		int id = 0;
		if (databaseType == 1) {
			id = doesRowExistAlready_MySql(columns, values);
		} else if (databaseType == 2) {
			id = doesRowExistAlready_MsSql(columns, values);
		}
		
		return id;
	}
	
	public int doesRowExistAlready_MySql(String[] columns, String[] values) {
		
		// get idents
		String whereQuery = getIdentiesWhereQuery(columns, values);
		
		String query = "SELECT ImportId FROM `" + database + "`.`" + table + "` " +
				"WHERE " + whereQuery + " LIMIT 0,1";
		
		int id = getQueryIdent(query);
		
		return id;
	}

	public int doesRowExistAlready_MsSql(String[] columns, String[] values) {
		
		// get idents
		String whereQuery = getIdentiesWhereQuery(columns, values);
		
		String query = "SELECT TOP 1 ImportId FROM " + database + "." + tableSchema + "." + table + " " +
				"WHERE " + whereQuery;
		
		int id = getQueryIdent(query);
		
		return id;
	}
	

	
	private String getQuery_Update_MySql(String[] columns, String[] values, int id) {
		
		String q = "";
		for (int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c = columns[i];
			
			try {
				v = values[i];
			} catch (Exception e) {
				v = "";
			}
			
			v = escapeForSql(v);
			
			q += "`" + c + "`='" + v + "'";

			if(i < columns.length-1) {
				q += ", ";
			}
		}
		
		String s = "UPDATE `" + database + "`.`" + table + "` " +
				"SET DateUpdated=NOW(), "+q+" " +
				"WHERE (ImportID='"+id+"');";
		
		return s;
	}
	
	private String getQuery_Update_MsSql(String[] columns, String[] values, int id) {
		
		String q = "";
		for (int i=0; i < columns.length; i++) {
			
			String c = "";
			String v = "";
			
			c = columns[i];
			
			try {
				v = values[i];
			} catch (Exception e) {
				v = "";
			}
			
			v = escapeForSql(v);
			
			q += "[" + c + "]='" + v + "'";
			
			if(i < columns.length-1) {
				q += ", ";
			}
		}
		
		String s = "UPDATE " + database + "." + tableSchema + "." + table + " " +
				"SET DateUpdated=GETDATE(), "+q+" " +
				"WHERE (ImportID='"+id+"');";
		
		return s;
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
			query = "SELECT COUNT(*) AS Total FROM " + database + "." + tableSchema + "." + table + " " +
					"WHERE  ([" + column + "] IS NOT NULL);"; // TODO - confirm not null... stinking can't use aggregate on text types...
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









