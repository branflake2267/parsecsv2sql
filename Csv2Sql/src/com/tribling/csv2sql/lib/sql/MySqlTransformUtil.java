package com.tribling.csv2sql.lib.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.lib.StringUtil;

public class MySqlTransformUtil extends MySqlQueryUtil {

  public MySqlTransformUtil() {
  }

  /**
   * fix table name
   * 
   * @param table
   * @return
   */
  public static String fixTableName(String table) {

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
   * @param dd
   * @param table
   * @param primaryKeyName
   */
  public static void createTable(DatabaseData dd, String table, String primaryKeyName) {
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
    String sql = "CREATE TABLE `" + dd.getDatabase() + "`.`" + table + "` " +
    	"(" + 
        "`" + primaryKeyName + "` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`" + primaryKeyName + "`) " + 
      ") ENGINE = MyISAM;";
    update(dd, sql);
  }
  
  /**
   * does table exist?
   * 
   * @param dd
   * @param table
   * @return
   */
  public static boolean doesTableExist(DatabaseData dd, String table) {
    String query = "SHOW TABLES FROM `" + dd.getDatabase() + "` LIKE '" + table + "';";
    return queryStringAndConvertToBoolean(dd, query);
  }
  
  /**
   * does column name exist in table?
   * 
   * @param dd
   * @param table
   * @param column
   * @return
   */
  public static boolean doesColumnExist(DatabaseData dd, ColumnData column) {
    
    String table = column.getTable();
    
    if (table == null | column == null) {
      return false;
    }
    if (table.length() == 0 | column.getColumnName().length() == 0) {
      return false;
    }
    String sql = "SHOW COLUMNS FROM `" + table + "` FROM `" + dd.getDatabase() + "` LIKE '" + column.getColumnName() + "';";
    boolean r = queryStringAndConvertToBoolean(dd, sql);
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
  public static ColumnData queryColumn(DatabaseData dd, String table, String columnName) {
    String where = "`FIELD`='" + columnName + "'";
    ColumnData[] columnData = queryColumns(dd, table, where);
    ColumnData r = null;
    if (columnData != null && columnData.length > 0) {
      r = columnData[0];
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
  public static ColumnData[] queryColumns(DatabaseData dd, String table, String where) {
    if (where != null && where.toLowerCase().contains("where") == false) {
      where = "WHERE " + where;
    } else if (where == null) {
      where = "";
    }
    String sql = "SHOW COLUMNS FROM `" + table + "` FROM `" + dd.getDatabase() + "` " + where + ";";
    ColumnData[] columns = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      columns = new ColumnData[getResultSetSize(result)];
      int i = 0;
      while (result.next()) {
        columns[i] = new ColumnData();
        columns[i].setTable(table);
        columns[i].setColumnName(result.getString(1));
        columns[i].setType(result.getString(2));
        //(3) null or not
        //(4) Key
        if (result.getString("Key") != null && 
            result.getString("Key").matches("PRI") == true) {
          columns[i].setIsPrimaryKey(true);
        }
        i++;
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryColumns(): " + sql);
      e.printStackTrace();
    }
    return columns;
  }
  
  public static ColumnData queryPrimaryKey(DatabaseData dd, String table) {
    String sql = "SHOW COLUMNS FROM `" + table + "` FROM `" + dd.getDatabase() + "` WHERE `Key`='PRI';";
    ColumnData column = new ColumnData();
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        column.setIsPrimaryKey(true);
        column.setColumnName(result.getString(1));
        column.setType(result.getString(2));
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
      conn = null;
    } catch (SQLException e) {
      System.err.println("Error: queryPrimaryKey(): " + sql);
      e.printStackTrace();
    } 
    return column;
  }

  /**
   * create columns
   * 
   * @param dd
   * @param columnData
   */
  public static ColumnData[] createColumn(DatabaseData dd, ColumnData[] columnData) {
    if (columnData == null) {
      return null;
    }
    for (int i=0; i < columnData.length; i++) {
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
  public static ColumnData createColumn(DatabaseData dd, ColumnData columnData) {
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

    boolean exist = doesColumnExist(dd, columnData);    
    if (exist == true) {
      ColumnData exCol = queryColumn(dd, table, columnData.getColumnName());
      columnData.setType(exCol.getType());
      return columnData;
    }

    if (type == null) {
      type = "TEXT DEFAULT NULL";
    }
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + table + "` ADD COLUMN `" + columnData.getColumnName() + "`  " + type + ";";
    update(dd, sql);
    
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
  public static void createColumn(DatabaseData dd, ColumnData column, int columnType, String length) {
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
  public static boolean doesIndexExist(DatabaseData dd, String table, String indexName) {
    String sql = "SHOW INDEX FROM `" + table + "` FROM `" + dd.getDatabase() + "` WHERE (`Key_name`= '" + indexName + "')";
    return queryStringAndConvertToBoolean(dd, sql);
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
  public static void createIndex(DatabaseData dd, String table, String indexName, String indexColumns, int indexKind) {

    // Index name length limitation
    if (indexName.length() > 30) {
      indexName = indexName.substring(0,30);
    }
    
    if (doesIndexExist(dd, table, indexName) == true) {
      return;
    }
    
    String kind = "";
    if (indexKind == ColumnData.INDEXKIND_DEFAULT | indexKind == 0) {
      kind = ""; // default is nothing stated
    } else if (indexKind == ColumnData.INDEXKIND_FULLTEXT) {
      kind = "FULLTEXT";
    }
    
    // if the column is a text column, set the index length and its not full text
    // ALTER TABLE `test`.`test` ADD INDEX `new_index1`(`myTxt`(900));
    // ALTER TABLE `test`.`test` ADD INDEX `new_index2`(`smallInt`, `myTxt`(900));
    // This should come in on indexColumns

    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + table + "` " + 
      "ADD " + kind + " INDEX `" + indexName + "`(" + indexColumns + ");";
      
    update(dd, sql);
  }
  
  /**
   * create index for identities
   * @param dd
   * @param columnData - all the columnData - will get the identities columns from in them
   */
  public static void createIndex_forIdentities(DatabaseData dd, ColumnData[] columnData) {

    ColumnData[] identities = ColumnData.getColumns_Identities(columnData);

    String indexes = "";
    for (int i = 0; i < identities.length; i++) {
 
      indexes += "`" + columnData[i].getColumnName() + "`";
        
      if (i < identities.length - 1) {
        indexes += ", ";
      }
    }
    
    // create the index
    String indexName = "auto_identies";
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
  public static void deleteColumn(DatabaseData dd, ColumnData columnData) {
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + columnData.getTable() + "` " +
    		"DROP COLUMN `" + columnData.getColumnName() + "`;";
    update(dd, sql);
  }
  
  /**
   * delete columns
   * 
   * @param dd
   * @param table
   * @param columnData
   */
  public static void deleteColumns(DatabaseData dd, ColumnData[] columnData) {
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + columnData[0].getTable() + "` ";
    for (int i=0; i < columnData.length; i++) {
      String column = columnData[i].getColumnName();
      sql += "DROP COLUMN `" + column + "`";
      if (i < columnData.length-1) {
        sql += ", ";
      }
    }
    update(dd, sql);
  }
  
  /**
   * delete columns with no data
   *   first check each column, then after that, then go through and delete the columns
   * @param dd
   * @param table
   * @param pruneColumnData - skip these columns
   */
  public static void deleteEmptyColumns(DatabaseData dd, String table, ColumnData[] pruneColumnData) {
    if (table == null) {
      return;
    }
    // get all columns of table
    ColumnData[] columnData = MySqlTransformUtil.queryColumns(dd, table, null);
    columnData = ColumnData.prune(columnData, pruneColumnData);
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
  public static long queryColumnCharactersLongestLength(DatabaseData dd, String table, ColumnData column) {
    String sql = "SELECT MAX(LENGTH(`" + column.getColumnName() + "`)) FROM " + dd.getDatabase() + "." + table + ";";
    return queryLong(dd, sql);
  }

  /**
   * show create table script
   * 
   * @param dd
   * @param table
   * @return
   */
  public static String showCreateTable(DatabaseData dd, String table) {
    String sql = "SHOW CREATE TABLE `" + dd.getDatabase() + "`.`" + table + "`";
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
  public static void dropTable(DatabaseData dd, String table) {
    String sql = "DROP TABLE IF EXISTS `" + dd.getDatabase() + "`.`" + table + "`;";
    update(dd, sql);
  }
  
  /**
   * does a column contain any data in it?
   * 
   * @param dd
   * @param columnData
   * @return
   */
  public static boolean doesColumnContainData(DatabaseData dd, ColumnData columnData) {
    String sql = "SELECT COUNT(`" + columnData.getColumnName() + "`) AS Total " +
    		"FROM `" + dd.getDatabase() + "`.`" + columnData.getTable() + "` " + 
    		"WHERE (`" + columnData.getColumnName() + "` != '');";
    int c = MySqlQueryUtil.queryInteger(dd, sql);
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
  public static String[] deleteIndexForColumn(DatabaseData dd, ColumnData columnData) {
    if (columnData == null) {
      return null;
    }
    
    String[] indexesToRestore = showCreateIndex(dd, columnData);
    
    String table = columnData.getTable();
    String sql = "SHOW INDEX FROM `" + dd.getDatabase() + "`.`" + columnData.getTable() + "` " + 
      "WHERE (Key_name != 'Primary') AND (Column_name = '" + columnData.getColumnName() + "')";
      
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
      e.printStackTrace();
    }

    return indexesToRestore;
  }
    
  /**
   * delete index
   * 
   * @param dd
   * @param table
   * @param indexName
   */
  public static void deleteIndex(DatabaseData dd, String table, String indexName) {
    String sql = "DROP INDEX `" + indexName + "` ON `" + table + "`;";
    MySqlQueryUtil.update(dd, sql);
  }
  
  /**
   * alter column
   *   delete the indexes on the column, then restore them on alter
   * @param dd
   * @param columnData
   */
  public static void alterColumn(DatabaseData dd, ColumnData columnData) {
    
    // TODO when altering a column from text to varchar and it has an index length on it
    
    String[] sqlIndexRestore = deleteIndexForColumn(dd, columnData);
    String indexSql = StringUtil.toCsv_NoQuotes(sqlIndexRestore);
    
    String modifyColumn = "`" + columnData.getColumnName() + "` " + columnData.getType();
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + columnData.getTable() + "` MODIFY COLUMN " + modifyColumn + " ";
    
    if (indexSql != null) {
      sql += ", " + indexSql;
    }
    System.out.println("alterColum(): " + sql);
    MySqlQueryUtil.update(dd, sql);
  }
  
  public static String[] showCreateIndex(DatabaseData dd, ColumnData columnData) {

    String showCreateTable = showCreateTable(dd, columnData.getTable());
  
    String regex = "KEY[\040]+`.*?`" + columnData.getColumnName() + "`.*?\n";
    
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
      System.out.println("findMatch: regex error");
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
  
  private static String changeIndexFromTextToVarchar(ColumnData columnData, String index) {
    
    boolean change = false;
    if (columnData.getType().toLowerCase().contains("text") == false) {
      change = true;
    }
    
    // does the index have a index length?
    if (change == true && index.matches(".*([0-9]+).*\n") == true) {
      index = index.replaceFirst("(\\([0-9]+\\)", "");
    } 
    
    return index;
  }
  
  
}
