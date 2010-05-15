package org.gonevertical.dts.lib.sql.transformlib;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.data.StatData;
import org.gonevertical.dts.data.UserDbData;
import org.gonevertical.dts.lib.StringUtil;
import org.gonevertical.dts.lib.sql.columnlib.MsSqlColumnLib;
import org.gonevertical.dts.lib.sql.querylib.MsSqlQueryLib;


public class OracleTransformLib implements TransformLib {
	
	private Logger logger = Logger.getLogger(OracleTransformLib.class);

  // supporting library
  private MsSqlQueryLib ql = new MsSqlQueryLib();
  
	private StatData stats;
  
  public OracleTransformLib() {
  }
  
  public void setStats(StatData stats) {
  	this.stats = stats;
  	ql.setStats(stats);
  }

  private void setTrackSql(String sql) {
  	if (stats == null) {
  		return;
  	}
  	stats.setTrackSql(sql);
  }

  private void setTrackError(String error) {
  	if (stats == null) {
  		return;
  	}
  	stats.setTrackError(error);
  }

  /**
   * fix table name
   * 
   * @param table
   * @return
   */
  public String fixTableName(String table) {

    // max table length name
    if (table.length() > 64) {
      table = table.substring(0, 63);
    }
    table = table.trim();

    table = table.replaceAll("#", "_Num");
    table = table.replaceAll("%", "_per");
    table = table.replaceAll(".", "_");
    table = table.replaceAll(" ", "_");
    
    table = table.replaceAll("[^\\w]", "");
    table = table.replaceAll("[\r\n\t]", "");
    table = table.replaceAll("(\\W)", "");

    return table;
  }
  
  /**
   * simple create table to work with
   * 
   * TODO - what if the schema does not exist, create it?
   * 
   * @param dd
   * @param table
   * @param primaryKeyName
   */
  // DONE
  public void createTable(DatabaseData dd, String table, String primaryKeyName) {
    if (table == null | primaryKeyName == null) {
      return;
    }
    if (table.length() == 0 | primaryKeyName.length() == 0) {
      return;
    }
    boolean doesExist = doesTableExist(dd, table);
    if (doesExist == true) {
      return;
    }
    
    String sql = "CREATE TABLE [" + dd.getTableSchema() + "].[" + table + "] ( " +
    		"[" + primaryKeyName + "] [BIGINT] IDENTITY(1,1) NOT NULL, " +
    	  "CONSTRAINT [PK_" + table + "] PRIMARY KEY CLUSTERED " + 
    	  "( [" + primaryKeyName + "] ASC )  " + // WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
    		") ON [PRIMARY] ;";
  
    System.out.println("createTable: " + sql);
    ql.update(dd, sql);
  }
  
  /**
   * does table exist?
   * 
   * @param dd
   * @param table
   * @return
   */
  // DONE
  public boolean doesTableExist(DatabaseData dd, String table) {   
  	String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " + 
  		"WHERE " +
  		"TABLE_CATALOG = '" + dd.getDatabase() + "' AND " + 
  		"TABLE_SCHEMA = '" + dd.getTableSchema() + "' AND " + 
  		"TABLE_NAME = '" + table + "'";
  	return ql.queryStringAndConvertToBoolean(dd, sql);
  }
  
  /**
   * does column name exist in table?
   * 
   * @param dd
   * @param table
   * @param columnData
   * @return
   */
  // DONE
  public boolean doesColumnExist(DatabaseData dd, ColumnData columnData) {
    
    String table = columnData.getTable();
    
    if (table == null | columnData == null) {
      return false;
    }
    if (table.length() == 0 | columnData.getColumnName().length() == 0) {
      return false;
    }
    
    String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " + 
			"WHERE " +
			"TABLE_CATALOG = '" + dd.getDatabase() + "' AND " + 
			"TABLE_SCHEMA = '" + dd.getTableSchema() + "' AND " + 
			"TABLE_NAME = '" + columnData.getTable() + "' AND " +
			"COLUMN_NAME = '" + columnData.getName() + "';";
    
    boolean r = ql.queryStringAndConvertToBoolean(dd, sql);
    return r;
  }
  
  /**
   * query column
   * 
   * @param dd
   * @param table
   * @param columnName
   * @return
   */
  // DONE
  public ColumnData queryColumn(DatabaseData dd, String table, String columnName) {
    String where = "[FIELD]='" + columnName + "'";
    ColumnData[] c = queryColumns(dd, table, where);
    ColumnData r = null;
    if (c != null && c.length > 0) {
      r = c[0];
    }
    return r;
  }
  
  /**
   * query column
   * 
   * @param dd
   * @param columnData
   * @return
   */
  // DONE
  public ColumnData queryColumn(DatabaseData dd, ColumnData columnData) {
    String where = "[FIELD]='" + columnData.getColumnName() + "'";
    ColumnData[] c = queryColumns(dd, columnData.getTable(), where);
    ColumnData r = null;
    if (c != null && c.length > 0) {
      r = c[0];
    }
    return r;
  }
  
  /**
   * get columns 
   * 
   * @param dd
   * @param table
   * @param where - ex: where="`FIELD` LIKE '%myCol%'"; 
   * @return
   */
  // almost done
  public ColumnData[] queryColumns(DatabaseData dd, String table, String where) {
    
    // TODO
    if (where != null && where.toLowerCase().contains("where") == false) {
      where = " AND " + where;
    } else if (where == null) {
      where = "";
    }
    
    String sql = "SELECT " +
      "COLUMN_NAME, " +
      "DATA_TYPE, " +
      "DATA_LENGTH, DATA_PRECISION, DATA_SCALE " + 
      "FROM ALL_TABLES " + 
      "WHERE " +
      "TABLE_NAME = '" + table + "' " + where + "";

    setTrackSql(sql);
    ArrayList<ColumnData> acolumns = new ArrayList<ColumnData>();
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
 
        ColumnData c = new ColumnData();
        c.setTable(table);
        c.setName(result.getString("COLUMN_NAME"));
        String type = result.getString("DATA_TYPE");
        
        if (type.contains("dec")) {
          type = type + "("+result.getInt("DATA_PRECISION") + ", " + result.getInt("DATA_SCALE") + ")";
        } else {
          type = type + "(" + result.getInt("DATA_LENGTH") + ")";
        }

        // TODO translate types
        if (type.toLowerCase().contains("date") == true) {
          type = "datetime";
          
        } 
        
        if (type.toLowerCase().contains("number") == true) {
          type = "bigint";
          
        } 
        
        if (type.toLowerCase().contains("varchar2") == true) {
          type = type.toLowerCase().replace("varchar2", "varchar");
        }
        
        c.setType(type);
        
        // is it a pri key
        if (queryIsColumnPrimarykey(dd, c) == true) {
          c.setIsPrimaryKey(true);
        }
  
        acolumns.add(c);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryColumns(): " + sql);
      setTrackError(e.toString());
      ///e.printStackTrace();
      
      // try this for earlier version
      acolumns = queryColumns_LessVer(dd, table, where);
    }
    ColumnData[] columns = new ColumnData[acolumns.size()];
    acolumns.toArray(columns);
    return columns; 
  }
  
  /**
   * different version
   * 
   * @param dd
   * @param table
   * @param where
   * @return
   */
  public ArrayList<ColumnData> queryColumns_LessVer(DatabaseData dd, String table, String where) {
    
    if (where != null && where.length() == 0) {
      where = null;
    }
    
    // TODO
    if (where != null && where.toLowerCase().contains("where") == false) {
      where = " AND " + where;
    } else if (where == null) {
      where = "";
    }
    
    String sql = "SELECT " +
      "COLUMN_NAME, " +
      "DATA_TYPE, " +
      "DATA_LENGTH, DATA_PRECISION, DATA_SCALE " + 
      "FROM ALL_TAB_COLUMNS " + // different
      "WHERE " +
      "TABLE_NAME = '" + table + "' " + where + "";

    setTrackSql(sql);
    ArrayList<ColumnData> acolumns = new ArrayList<ColumnData>();
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
 
        ColumnData c = new ColumnData();
        c.setTable(table);
        c.setName(result.getString("COLUMN_NAME"));
        String type = result.getString("DATA_TYPE");
        
        if (type.contains("dec")) {
          type = type + "("+result.getInt("DATA_PRECISION") + ", " + result.getInt("DATA_SCALE") + ")";
        } else {
          type = type + "(" + result.getInt("DATA_LENGTH") + ")";
        }

        // TODO translate types
        if (type.toLowerCase().contains("date") == true) {
          type = "datetime";
          
        } 
        
        if (type.toLowerCase().contains("number") == true) {
          type = "bigint";
          
        } 
        
        if (type.toLowerCase().contains("varchar2") == true) {
          type = type.toLowerCase().replace("varchar2", "varchar");
        }
        
        c.setType(type);
        
        // is it a pri key
        if (queryIsColumnPrimarykey(dd, c) == true) {
          c.setIsPrimaryKey(true);
        }
  
        acolumns.add(c);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryColumns(): " + sql);
      setTrackError(e.toString());
      e.printStackTrace();
    }
    
    return acolumns; 
  }
  
  // DONE
  public ColumnData queryPrimaryKey(DatabaseData dd, String table) {

    String sql = "SELECT COLUMN_NAME " +
  		"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " + 
  		"WHERE " +
  		"TABLE_CATALOG='" + dd.getDatabase() + "' AND " + 
  		"TABLE_SCHEMA='" + dd.getTableSchema() + "' AND " + 
  		"TABLE_NAME='" + table + "';";
    
    setTrackSql(sql);
    String priColName = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
      	priColName = result.getString(1);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryPrimaryKey(): " + sql);
      setTrackError(e.toString());
      e.printStackTrace();
    } 
    if (priColName == null) {
    	return null;
    }
    
    ColumnData[] pc = queryColumns(dd, table, "WHERE [COLUMN_NAME]='" + priColName + "'");
    
    if (pc.length == 0) {
    	return null;
    }
    
    return pc[0];
  }
  
  // DONE
  public boolean queryIsColumnPrimarykey(DatabaseData dd, ColumnData columnData) {
    
    String sql = "SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME IN " +
      "(SELECT CONSTRAINT_NAME " +
      "FROM ALL_CONSTRAINTS WHERE TABLE_NAME='" + columnData.getTable() + "' and CONSTRAINT_TYPE='P') " +
      "AND COLUMN_NAME='" + columnData.getName() + "' ";
    
  	setTrackSql(sql);
    String columnName = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        columnName = result.getString(1);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryPrimaryKey(): " + sql);
      setTrackError(e.toString());
      e.printStackTrace();
    } 
    
    boolean b = false;
    if (columnName == null) {
    	b = false;
    } else if (columnName.equals(columnData.getName())) {
    	b = true;
    }
    
    return b;
  }

  /**
   * create columns
   * 
   * @param dd
   * @param columnData
   */
  // DONE
  public ColumnData[] createColumn(DatabaseData dd, ColumnData[] columnData) {
    if (columnData == null) {
      return null;
    }
    for (int i=0; i < columnData.length; i++) {
      if (columnData[i].getColumnName() == null || columnData[i].getColumnName().trim().length() == 0) {
        columnData[i].setColumnName("c" + i);
      }
      columnData[i] = createColumn(dd, columnData[i]);
    }
    return columnData;
  }
  
  /**
   * create column
   * 
   * @param dd
   * @param table
   * @param columnData - column name and type - varchar(255) or TEXT or TEXT DEFAULT NULL or INTEGER DEFAULT 0
   */
  // DONE
  public ColumnData createColumn(DatabaseData dd, ColumnData columnData) {
    if (columnData == null | columnData.getColumnName().length() == 0) {
      return null;
    }
    
    if (columnData.getTable() == null) {
      System.out.println("createColumn(): ColumnData doesn't have a table set");
    }
    
    String type = columnData.getType();
    if (type == null | type.length() == 0) {
      type = "TEXT DEFAULT NULL";
    }
    
    String table = columnData.getTable();
    
    // be sure the column name doesn't have weird characters in it
    columnData.fixName();

    boolean exist = doesColumnExist(dd, columnData);   //DONE 
    if (exist == true) {
      ColumnData exCol = queryColumn(dd, table, columnData.getColumnName());
      columnData.setType(exCol.getType());
      return columnData;
    }

    if (type == null) {
      type = "TEXT DEFAULT NULL";
    }
    String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + table + "] " +
    		"ADD [" + columnData.getColumnName() + "]  " + type + ";";
    
    System.out.println("MySqlTransformUtil.createColumn(): " + sql);
    ql.update(dd, sql);
    
    return columnData;
  }
  
  /**
   * create a column by specifying a type and maybe length
   * 
   * @param dd
   * @param table
   * @param column
   * @param columnType
   * @param length - varchar(length), decimal(length)
   */
  // DONE
  public void createColumn(DatabaseData dd, ColumnData column, int columnType, String length) {
    if (length == null | length.length() == 0) {
      if (columnType == ColumnData.FIELDTYPE_VARCHAR) {
        length = "255";
      } else if (columnType == ColumnData.FIELDTYPE_DECIMAL) {
        length = "10,4";
      }
    }
    String type = "";
    if (columnType == ColumnData.FIELDTYPE_TEXT) {
      type = "TEXT DEFAULT NULL";
    } else if (columnType == ColumnData.FIELDTYPE_VARCHAR) {
      type = "VARCHAR(" + length + ") DEFAULT NULL";
    } else if (columnType == ColumnData.FIELDTYPE_INT) {
      type = "INT DEFAULT 0";
    } else if (columnType == ColumnData.FIELDTYPE_DECIMAL) {
      type = "DECIMAL(" + length + ") DEFAULT 0.0";
    } else if (columnType == ColumnData.FIELDTYPE_DATETIME) {
      type = "DATETIME DEFAULT NULL";
    } else {
      type = "TEXT DEFAULT NULL";
    }
    column.setType(type);
    
    createColumn(dd, column);
  }
  
  /**
   * does indexName exist
   * 
   * @param dd
   * @param table
   * @param indexName
   * @return
   */
  public boolean doesIndexExist(DatabaseData dd, String table, String indexName) {
    
  	String sql = "select name from sysindexes " +
  			"where name = '" + indexName + "' AND id = " +
  			"(select object_id from sys.tables where name = '" + table + "' and schema_id = (select top 1 schema_id from sys.schemas where name = '" + dd.getTableSchema() + "'))";
  	
    return ql.queryStringAndConvertToBoolean(dd, sql);
  }
  
  /**
   * create Index
   * 
   * @param dd
   * @param table
   * @param indexName 
   * @param indexColumns
   * @param indexKind 0 for default | ColumnData.INDEXKIND_DEFAULT | ColumnData.INDEXKIND_FULLTEXT
   */
  public void createIndex(DatabaseData dd, String table, String indexName, String indexColumns, int indexKind) {

    // Index name length limitation
    if (indexName.length() > 30) {
      indexName = indexName.substring(0,30);
    }
    
    if (doesIndexExist(dd, table, indexName) == true) {
      return;
    }
    
    // TODO hmmmmm
    String kind = "";
    if (indexKind == ColumnData.INDEXKIND_DEFAULT | indexKind == 0) {
      kind = ""; // default is nothing stated
    } else if (indexKind == ColumnData.INDEXKIND_FULLTEXT) {
      kind = "FULLTEXT";
    }
    
    String sql = "CREATE INDEX [" + indexName + "] ON [" + dd.getTableSchema() + "].[" + table + "] (" + indexColumns + ");";
      
    ql.update(dd, sql);
  }
  
  /**
   * create index for identities
   * 
   * @param dd
   * @param columnData - all the columnData - will get the identities columns from in them
   */
  public void createIndex_forIdentities(DatabaseData dd, ColumnData[] columnData, String indexName) {
                                         
    ColumnData[] identities = new MsSqlColumnLib().getColumns_Identities(columnData);

    String indexes = "";
    for (int i = 0; i < identities.length; i++) {
 
      indexes += "[" + columnData[i].getColumnName() + "]";
        
      if (i < identities.length - 1) {
        indexes += ", ";
      }
    }
    
    // create the index
    String table = columnData[0].getTable();
    createIndex(dd, table, indexName, indexes, ColumnData.INDEXKIND_DEFAULT);
  }
  
  /**
   * delete column
   * 
   * @param dd
   * @param table
   * @param columnData
   */
  //DONE
  public void deleteColumn(DatabaseData dd, ColumnData columnData) {
    String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + columnData.getTable() + "] " +
    		"DROP COLUMN [" + columnData.getColumnName() + "];";
    ql.update(dd, sql);
  }
  
  /**
   * delete columns
   * 
   * @param dd
   * @param table
   * @param columnData
   */
  //DONE
  public void deleteColumns(DatabaseData dd, ColumnData[] columnData) {
    String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + columnData[0].getTable() + "] ";
    for (int i=0; i < columnData.length; i++) {
      String column = columnData[i].getColumnName();
      sql += "DROP COLUMN [" + column + "]";
      if (i < columnData.length-1) {
        sql += ", ";
      }
    }
    ql.update(dd, sql);
  }
  
  /**
   * delete columns with no data
   *   first check each column, then after that, then go through and delete the columns
   * @param dd
   * @param table
   * @param pruneColumnData - skip these columns
   */
  //DONE
  public void deleteEmptyColumns(DatabaseData dd, String table, ColumnData[] pruneColumnData) {
    if (table == null) {
      return;
    }
    // get all columns of table
    ColumnData[] columnData = queryColumns(dd, table, null);
    columnData = new MsSqlColumnLib().prune(columnData, pruneColumnData);
    ArrayList<ColumnData> deleteCols = new ArrayList<ColumnData>();
    int i2 = columnData.length - 1; // count down total
    for (int i = 0; i < columnData.length; i++) {

      System.out.println(i2 + ". checking column is Empty?: " + columnData[i].getColumnName() + " for data.");
      
      if (doesColumnContainData(dd, columnData[i]) == false) {
        deleteCols.add(columnData[i]);
      }

      // count down
      i2--;
    }
    ColumnData[] odelCols = new ColumnData[deleteCols.size()];
    deleteCols.toArray(odelCols);
    deleteColumns(dd, odelCols);
  }

  /**
   * query column characters longest length,
   *    it looks through the entire columns recordset and finds the longest string's character count
   * @param dd
   * @param table
   * @param column
   * @return
   */
  public long queryColumnCharactersLongestLength(DatabaseData dd, String table, ColumnData column) {
    String sql = "SELECT MAX(LENGTH([" + column.getColumnName() + "])) FROM " + dd.getDatabase() + "." + table + ";";
    return ql.queryLong(dd, sql);
  }

  /**
   * show create table script
   * 
   * @param dd
   * @param table
   * @return
   */
  public String showCreateTable(DatabaseData dd, String table) {
    String sql = "SHOW CREATE TABLE [" + dd.getDatabase() + "].[" + table + "]";
    setTrackSql(sql);
    String s = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        s = result.getString(2);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryString(): " + sql);
      setTrackError(e.toString());
      e.printStackTrace();
    } 
    return s;
  }
  
  /**
   * drop table, delete it
   * 
   * @param dd
   * @param table
   */
  public void dropTable(DatabaseData dd, String table) {
  	
  	boolean exists = doesTableExist(dd, table);
  	if (exists == false) {
  		return;
  	}
  	
    String sql = "DROP TABLE [" + dd.getDatabase() + "].[" + dd.getTableSchema() + "].[" + table + "];";
    ql.update(dd, sql);
    
    System.out.println("dropped table " + table);
  }
  
  /**
   * does a column contain any data in it?
   * 
   * @param dd
   * @param columnData
   * @return
   */
  public boolean doesColumnContainData(DatabaseData dd, ColumnData columnData) {
    String sql = "SELECT COUNT([" + columnData.getColumnName() + "]) AS Total " +
    		"FROM [" + dd.getDatabase() + "].[" + columnData.getTable() + "] " + 
    		"WHERE ([" + columnData.getColumnName() + "] != '');";
    int c = ql.queryInteger(dd, sql);
    boolean b = true;
    if (c == 0) {
      b = false;
    }
    return b;
  }
  
  /**
   * delete all indexing for a column. find all the indexing that uses this column 
   *
   * @param dd
   * @param columnData
   * @return indexes, for recreation
   */
  public String[] deleteIndexForColumn(DatabaseData dd, ColumnData columnData) {
    if (columnData == null) {
      return null;
    }
    
    String[] indexesToRestore = showCreateIndex(dd, columnData);
    
    String table = columnData.getTable();
    String sql = "SHOW INDEX FROM [" + dd.getDatabase() + "`.`" + columnData.getTable() + "] " + 
      "WHERE (Key_name != 'Primary') AND (Column_name = '" + columnData.getColumnName() + "')";
      
    setTrackSql(sql);
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        String indexName = result.getString(3);
        deleteIndex(dd, table, indexName);
      }
      select.close();
      select = null;
      result.close();
      result = null;
    } catch (SQLException e) {
      System.err.println("Error: deleteIndexForColumn(): " + sql);
      setTrackError(e.toString());
      e.printStackTrace();
    }

    return indexesToRestore;
  }
  
  /**
   * delete indexes and returns indexes deleted
   * 
   * @param dd
   * @param columnData
   * @return - indexes deleted
   */
  public String[] deleteIndexForColumn(DatabaseData dd, ColumnData[] columnData) {
    
    ArrayList<String[]> index = new ArrayList<String[]>();
    for(int i=0; i < columnData.length; i++) {
      String[] inToRestore = deleteIndexForColumn(dd, columnData[i]);
      if (inToRestore.length > 0) {
        index.add(inToRestore);
      }
    }
    
    ArrayList<String> in = new ArrayList<String>();
    for (int i=0; i < index.size(); i++) {
      String[] rr = index.get(i);
      for (int b=0; b < rr.length; b++) {
        in.add(rr[b]);
      }
    }
    
    String[] r = new String[in.size()];
    in.toArray(r);
    
    return r;
  }
    
  /**
   * delete index
   * 
   * @param dd
   * @param table
   * @param indexName
   */
  public void deleteIndex(DatabaseData dd, String table, String indexName) {
    String sql = "DROP INDEX [" + indexName + "] ON [" + table + "];";
    System.out.println("deleteIndex: " + sql);
    ql.update(dd, sql);
  }
  
  /**
   * alter column
   *   delete the indexes on the column, then restore them on alter
   * @param dd
   * @param columnData
   */
  public void alterColumn(DatabaseData dd, ColumnData columnData) {
  	
  	// TODO - come back and figure out a way to get indexes and delete them
    //String[] sqlIndexRestore = deleteIndexForColumn(dd, columnData);
    //String indexSql = StringUtil.toCsv_NoQuotes(sqlIndexRestore);
    
  	dropContraint(dd, columnData);
  	
    String modifyColumn = "[" + columnData.getColumnName() + "] " + columnData.getType();
    String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + columnData.getTable() + "] ALTER COLUMN " + modifyColumn + " ";
    
    //if (indexSql != null) {
    //  sql += ", " + indexSql;
    //}
    System.out.println("Transform: alterColum(): " + sql);
    ql.update(dd, sql);
  }
  
  public void alterColumn(DatabaseData dd, ColumnData[] columnData) {
    if (columnData == null || columnData.length == 0) {
      return;
    }
    
    // TODO figure out a way to restore the index
    //String[] sqlIndexRestore = deleteIndexForColumn(dd, columnData);
   
    
    String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + columnData[0].getTable() + "] ";
    for (int i=0; i < columnData.length; i++) {
      String modifyColumn = "MODIFY COLUMN [" + columnData[i].getColumnName() + "] " + columnData[i].getType();
      sql += " " + modifyColumn + " ";
      if (i < columnData.length - 1) {
        sql += ",";
      }
    }
    
    //sqlIndexRestore = checkIndexTextLengths(columnData, sqlIndexRestore);
    //String indexSql = StringUtil.toCsv_NoQuotes(sqlIndexRestore);
    //if (indexSql != null) {
    //  sql += ", " + indexSql;
    //}
    System.out.println("Transform: alterColum(): " + sql);
    ql.update(dd, sql);
  }
  
  private String[] checkIndexTextLengths(ColumnData[] columnData, String[] indexes) {
    for (int i=0; i < indexes.length; i++) {
      indexes[i] = checkIndexTextLength(columnData, indexes[i]);
    }
    return indexes;
  }

 private String checkIndexTextLength(ColumnData[] columnData, String index) {
    
    System.out.println("index: " + index);
    // ADD INDEX `auto_identities` (`series_id`,`period`(330),`value`(330))
    if (index.contains(",") == true) {
      String colstr = StringUtil.getValue("\\((.*)\\)$", index);
      String[] cols = colstr.split(",");
      ColumnData[] newIndex = new ColumnData[cols.length];
      for (int i=0; i < cols.length; i++) {
        String colname = StringUtil.getValue("\\[(.*)\\]", cols[i]);
        System.out.println(colname);
        int cIndex = new MsSqlColumnLib().searchColumnByName_UsingComparator(columnData, colname);
        ColumnData c = columnData[cIndex];
        newIndex[i] = c; 
      }
      String csql = new MsSqlColumnLib().getSql_Index_Multi(newIndex);
      index = index.replaceAll("\\(.*\\)$", "(" + csql + ")");
      System.out.println("replace: " + index);
    }
   
    return index;
  }

  public String[] showCreateIndex(DatabaseData dd, ColumnData columnData) {

    String showCreateTable = showCreateTable(dd, columnData.getTable());
  
    String regex = "KEY[\040]+\\[.*?\\]" + columnData.getColumnName() + "\\].*?\n";
    
    ArrayList<String> indexes = new ArrayList<String>(); 
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(showCreateTable);
      while (m.find()) {
        String index = m.group();
        
        // if the index was a text and
        index = changeIndexFromTextToVarchar(columnData, index);
        
        
        indexes.add(index);
 
      }
    } catch (Exception e) {
      e.printStackTrace();
      setTrackError(e.toString());
      System.out.println("ERROR: showCreateIndex(): findMatch: regex error");
    }
  
    String[] r = new String[indexes.size()];
    for (int i=0; i < indexes.size(); i++) {
      String s = indexes.get(i);
      s = s.replaceAll("\n", "");
      s = s.replace("KEY", "ADD INDEX");
      if (s.matches(".*?,") == true) {
        s = s.substring(0,s.length() - 1);
        r[i] = s;
      } else {
        r[i] = s;
      }
    }
    return r;
  }
  
  /**
   * when going from text to varchar, take out the index byte length
   * @param columnData
   * @param index
   * @return
   */
  private String changeIndexFromTextToVarchar(ColumnData columnData, String index) {
    
    boolean change = false;
    if (columnData.getType().toLowerCase().contains("text") == false) {
      change = true;
    }
    
    // does the index have a index length?
    if (change == true && index.matches(".*([0-9]+).*\n") == true) {
      try {
        index = index.replaceFirst("(\\([0-9]+\\))", "");
      } catch (Exception e) {
      	setTrackError(e.toString());
        e.printStackTrace();
      }
    } 
    
    return index;
  }

  public String getType() {
    return "MsSql";
  }

  public void createUser(DatabaseData dd, UserDbData userData) {
    // TODO Auto-generated method stub
    
  }

  public void createUser(DatabaseData dd, String userName, String password,
      String host) {
    // TODO Auto-generated method stub
    
  }

  public boolean doesUserExist(DatabaseData dd, String userName,
      String password, String host) {
    // TODO Auto-generated method stub
    return false;
  }

  public String getConstraintName(DatabaseData dd, ColumnData columnData) {
  	String sql = "select name from sys.default_constraints where parent_column_id = " +
  			"(select column_id from sys.columns where name = '" + columnData.getName() + "' and object_id = " +
  			"(select object_id from sys.tables where name = '" + columnData.getTable() + "' and schema_id = " +
  			"(select top 1 schema_id from sys.schemas where name = '" + dd.getTableSchema() + "')))";
  	return ql.queryString(dd, sql);
  }
  
  public void dropContraint(DatabaseData dd, ColumnData columnData) {
  	String contraintName = getConstraintName(dd, columnData);
  	String sql = "ALTER TABLE [" + dd.getTableSchema() + "].[" + columnData.getTable() + "] " +
		"DROP CONSTRAINT [" + contraintName + "];";
  	ql.update(dd, sql);
  }

	@Override
  public String[] getTablesAll(DatabaseData dd) {
	  // TODO Auto-generated method stub
	  return null;
  }
  
}
