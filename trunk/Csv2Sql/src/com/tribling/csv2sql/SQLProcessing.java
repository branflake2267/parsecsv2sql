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

	int connLoadBalance = 0;
	protected Connection conn1;
	protected Connection conn2;
	
	// track where at in the importing
	private int index = 0;
	private int indexFile = 0;

	protected DestinationData dd = null;

	private boolean dropTableOff = false;
	
	// replace these fields
	private MatchFieldData[] matchFields;

	// what database brand?
	protected int databaseType;
	
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
	public void setDestinationData(DestinationData destinationData) {
		
		dd = destinationData;
	
		// test for db type
		this.databaseType = dd.getDbType();
		
		if (dd.database.length() == 0)  {
			System.err.println("ERROR: No Database: Whats the database name?");
			System.exit(1);
		}
		
		if (dd.username.length() == 0) {
			System.err.println("ERROR: No username: What is the sql database username?");
			System.exit(1);
		}
		
		if (dd.password.length() == 0) {
			System.err.println("ERROR: No password: What is the sql database password?");
			System.exit(1);
		}
		
		if (destinationData.host.length() == 0) {
			System.err.println("ERROR: No host: Where is the server located? ie. [IpAddress|host.domain.tld]");
			System.exit(1);
		}
		
		if (destinationData.port.length() == 0) {
			System.err.println("ERROR: No port: What doorway is the sql server behind? ie. [3306|1433]");
			System.exit(1);
		}
		
		if (destinationData.table.length() == 0) {
			System.err.println("ERROR: No destination table: What table do you want to import this data to?");
			System.exit(1);
		}
		
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
			query = "SHOW TABLES FROM `" + dd.database + "` LIKE '" + dd.table + "';";
		} else if (databaseType == 2) {
			query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
					"WHERE TABLE_CATALOG = '" + dd.database + "' AND " +
					"TABLE_SCHEMA = '" + dd.tableSchema + "' AND " +
					"TABLE_NAME = '" + dd.table + "'";
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
			
			if (dd.dropTable == true && dropTableOff == false) {
				dropTable();
			} else if (dd.dropTable == false && doesExist == false) {
				return;
			} else if (dropTableOff == true && doesExist == true) {
				return;
			}
			
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "CREATE TABLE `" + dd.database + "`.`" + dd.table + "` (" +
					"`ImportID` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`ImportID`) " +
					") ENGINE = MyISAM;";
		} else if (databaseType == 2) {
			query = "CREATE TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + " " +
					"( [ImportID] [INT] IDENTITY(1,1) NOT NULL);";
		}
		
		setUpdateQuery(query);
		
		// track changes by date - Create DateCreated column
		String column = "DateCreated";
		String type = "";
		if (databaseType == 1) {
			type = "DATETIME DEFAULT NULL";
		} else if (databaseType == 2) {
			type = "DATETIME NULL";
		}
		createColumn(column, type);
		
		// add DateUpdated column to track updates - Create DateUpdated column
		if (dd.checkForExistingRecordsAndUpdate == true) {
			column = "DateUpdated";
			type = "";
			if (databaseType == 1) {
				type = "DATETIME DEFAULT NULL";
			} else if (databaseType == 2) {
				type = "DATETIME NULL";
			}
			createColumn(column, type);
		} 
		
		// create indexes of identity columns listed
		if (dd.createIndexs == true) {
			createIndexes();
		}
		

	}
	
	/**
	 * sql drop table
	 */
	private void dropTable() {
		
		String query = "";
		if (databaseType == 1) {
			query = "DROP TABLE IF EXISTS `" + dd.database + "`.`" + dd.table + "`;";
		} else if (databaseType == 2) {
			if (isTableExist()) {
				query = "DROP TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + ";";
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
			query = "SHOW COLUMNS FROM `" + dd.table + "` FROM `" + dd.database + "` LIKE '" + column + "';";
		} else if (databaseType == 2) {
			query = "SELECT COLUMN_NAME FROM " + dd.database + ".INFORMATION_SCHEMA.Columns " +
					"WHERE TABLE_SCHEMA='" + dd.tableSchema + "' AND " +
					"TABLE_NAME='" + dd.table + "' AND " +
					"COLUMN_NAME = '" + column + "';";
		}
		
		boolean rtn = getBooleanQuery(query);
		
		return rtn;
	}
	
	
	protected String[] getColumns() {
		String query = "";
		if (databaseType == 1) {
			query = "SHOW COLUMNS FROM `" + dd.table + "` FROM `" + dd.database + "`;";
		} else if (databaseType == 2) {
			query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.Columns " +
					"WHERE TABLE_NAME ='" + dd.table + "' AND " +
					"TABLE_SCHEMA='" + dd.tableSchema + "' AND TABLE_CATALOG='" + dd.database + "'";
		}

		System.out.println("query: " + query);
		
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
		
		for (int i=0; i < columns.length; i++) {
			createColumn(columns[i]);
		}
		
		return columns;
	}
	
	/**
	 * Create a column with text default
	 * 
	 * Two reasons to start with text.
	 * 1. fields can have length > 255
	 * 2. 65535 length limit of sql insert
	 * NOTE: have to alter to varchar/text first then drill down further after that
	 *
	 * @param column
	 */
	private void createColumn(String column) {
		String type = "";
		if (databaseType == 1) {
			type = "TEXT DEFAULT NULL";
		} else if (databaseType == 2) {
			type = "TEXT NULL"; // VARCHAR(255) // TODO -aggregate functions don't work with this. Need to alter to text with lenths greather than text
		}
		createColumn(column,type);
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
	 * 
	 * @param column
	 * @param type
	 */
	private void createColumn(String column, String type) {
		
		column = fixName(column);
		
		boolean exist = isColumnExist(column);
		if(exist == true) {
			return;
		}

		// default column type for creation
		if (type == null) {
			if (databaseType == 1) { //mysql
				type = "TEXT";
			} else if (databaseType == 2) {
				// For some reason I don't undertand why I cant alter from text->varchar dump, mysql does it fine
				type = "VARHCAR(255)"; //mssql
			}
		}
		
		String query = "";
		if (databaseType == 1) {
			query = "ALTER TABLE `" + dd.database + "`.`" + dd.table + "` ADD COLUMN `" + column + "`  " + type + ";";
		} else if (databaseType == 2) {
			query = "ALTER TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + " ADD [" + column + "] " + type + ";";
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

		String url = "jdbc:mysql://" + dd.host + ":" + dd.port + "/";
		String driver = "com.mysql.jdbc.Driver";
		System.out.println("getConn_MySql: url:" + url + " user: " + dd.username + " driver: " + driver);
		
		Connection conn = null;
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url + dd.database, dd.username, dd.password);
		} catch (Exception e) {
			System.err.println("MySql Connection Error:");
			e.printStackTrace();
			System.out.println("Fix Connection.");
			System.exit(1);
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

		String url = "jdbc:sqlserver://" + dd.host + ";user=" + dd.username + ";password=" + dd.password + ";databaseName=" + dd.database + ";";
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		System.out.println("getConn_MsSql: url:" + url + " user: " + dd.username + " driver: " + driver);
		
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url);
		} catch (Exception e) {
			System.err.println("MsSql Connection Error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
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
            update.close();
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
	 * 
	 * 
	 * mysql table, and columns should be < 64 char
	 * 
	 * @param s
	 * @return
	 */
	private String fixName(String s) {
		
		if (s.length() > 64) { 
			s = s.substring(0, 63);
		}
		
		s = s.replace("'", "");
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
		if (dd.checkForExistingRecordsAndUpdate == true) {
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
		
		String s = "INSERT INTO " + dd.database + "." + dd.tableSchema + "." + dd.table + " (DateCreated, " + cs + ") " +
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
		
		String s = "INSERT INTO `" + dd.database + "`.`" + dd.table + "` " +
				"SET DateCreated=NOW(), "+q+";";
		
		return s;
	}
	
	/**
	 * create indexes of the identities given
	 */
	private void createIndexes() {
		
		if (dd.identityColumns == null) {
			System.out.println("skipping creating indexes, b/c there are no identiy columns listed");
			return;
		}
		
		String indexes = "";
		for(int i=0; i < dd.identityColumns.length; i++) {
			String column = dd.identityColumns[i].desinationField;
			createColumn(column, "VARCHAR(255)");
			
			indexes += "`" + column + "`";
			if (i < dd.identityColumns.length - 1) {
				indexes += ", ";
			}
		}
		createIndex(indexes);
	}
	
	/**
	 * create index for identities given
	 * 
	 * @param column
	 */
	private void createIndex(String s) {
	
		String indexName = "index_auto";
		String query = "";
		if (databaseType == 1) {
			query = "ALTER TABLE `" + dd.database + "`.`" + dd.table + "` " +
					"ADD INDEX `" + indexName + "` USING BTREE(" + s + ");";
		} else if (databaseType == 2) {
			// TODO - make this work!
			query = "ALTER TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + " " +
					"ADD INDEX [" + s + "]([" + indexName + "]) ;";
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
		
		IdentityData[] ident = new IdentityData[dd.identityColumns.length];
		for (int i=0; i < dd.identityColumns.length; i++) {
			String identColumnName = dd.identityColumns[i].desinationField;

			int index = searchArray(columns, identColumnName);
			
			ident[i] = new IdentityData();
			ident[i].column = dd.identityColumns[i].desinationField;
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
		
		String query = "SELECT ImportId FROM `" + dd.database + "`.`" + dd.table + "` " +
				"WHERE " + whereQuery + " LIMIT 0,1";
		
		int id = getQueryIdent(query);
		
		return id;
	}

	public int doesRowExistAlready_MsSql(String[] columns, String[] values) {
		
		// get idents
		String whereQuery = getIdentiesWhereQuery(columns, values);
		
		String query = "SELECT TOP 1 ImportId FROM " + dd.database + "." + dd.tableSchema + "." + dd.table + " " +
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
		
		String s = "UPDATE `" + dd.database + "`.`" + dd.table + "` " +
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
		
		String s = "UPDATE " + dd.database + "." + dd.tableSchema + "." + dd.table + " " +
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
		
		if (dd.deleteEmptyColumns == false) {
			System.out.println("Skipping deleting empty Columns: destinationData.deleteEmptyColumns=false");
		    return;
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
			query = "SELECT COUNT(`" + column + "`) AS Total FROM `" + dd.database + "`.`" + dd.table + "` " +
					"WHERE (`" + column + "` != '');";
		} else if (databaseType == 2) {
			query = "SELECT COUNT(*) AS Total FROM " + dd.database + "." + dd.tableSchema + "." + dd.table + " " +
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
			query = "ALTER TABLE `" + dd.database + "`.`" + dd.table + "` DROP COLUMN `" + column + "`;";
		} else if (databaseType == 2) {
			query = "ALTER TABLE " + dd.database + "." + dd.tableSchema + "." + dd.table + " DROP COLUMN " + column + ";";
		}
		
		setUpdateQuery(query);
	}
}









