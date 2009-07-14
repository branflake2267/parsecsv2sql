package com.tribling.csv2sql.lib.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;

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
    String query = "SHOW COLUMNS FROM `" + table + "` FROM `" + dd.getDatabase() + "` LIKE '" + column.getColumnName() + "';";
    boolean r = queryStringAndConvertToBoolean(dd, query);
    return r;
  }
  
  /**
   * get columns 
   * 
   * @param dd
   * @param table
   * @param where - ex: where="like '%myCol%'"; 
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
        columns[i].column = result.getString(1);
        columns[i].setType(result.getString(2));
        i++;
      }
      result.close();
      result = null;
      select.close();
      select = null;
    } catch (SQLException e) {
      System.err.println("Error: queryColumns(): " + sql);
      e.printStackTrace();
    } finally {
      dd.closeConnection();
    }
    return columns;
  }
  
  public static ColumnData queryPrimaryKey(DatabaseData dd, String table) {
    String sql = "SHOW COLUMNS FROM `" + table + "` FROM `" + dd.getDatabase() + "` `Key`='PRI';";
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
    } catch (SQLException e) {
      System.err.println("Error: queryPrimaryKey(): " + sql);
      e.printStackTrace();
    } finally {
      dd.closeConnection();
    }
    return column;
  }

  /**
   * create column
   * 
   * @param dd
   * @param table
   * @param column - column name and type - varchar(255) or TEXT or TEXT DEFAULT NULL or INTEGER DEFAULT 0
   */
  public static void createColumn(DatabaseData dd, ColumnData column) {
    if (column == null | column.getColumnName().length() == 0) {
      return;
    }
    
    String type = column.getType();
    if (type == null | type.length() == 0) {
      type = "TEXT DEFAULT NULL";
    }
    
    String table = column.getTable();
    
    // be sure the column name doesn't have weird characters in it
    column.fixName();

    boolean exist = doesColumnExist(dd, column);    
    if (exist == true) {
      return;
    }

    if (type == null) {
      type = "TEXT DEFAULT NULL";
    }
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + table + "` ADD COLUMN `" + column + "`  " + type + ";";
    update(dd, sql);
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
    } else if (columnType == ColumnData.FIELDTYPE_SMALLINT) {
      type = "SMALLINT DEFAULT 0";
    } else if (columnType == ColumnData.FIELDTYPE_INT) {
      type = "INT DEFAULT 0";
    } else if (columnType == ColumnData.FIELDTYPE_BITINT) {
      type = "BIGINT DEFAULT 0";
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
    String sql = "SHOW INDEX FROM `" + table + "` FROM `" + dd.getDatabase() + "` WHERE (Key_name = '" + indexName + "')";
    return queryStringAndConvertToBoolean(dd, sql);
  }
  
  /**
   * create Index
   * 
   * @param dd
   * @param table
   * @param indexName 
   * @param indexColumns
   * @param indexKind
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
    if (indexKind == ColumnData.INDEXKIND_DEFAULT) {
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
   * delete column
   * 
   * @param dd
   * @param table
   * @param column
   */
  public static void deleteColumn(DatabaseData dd, String table, ColumnData column) {
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + table + "` DROP COLUMN `" + column.getColumnName() + "`;";
    update(dd, sql);
  }
  
  /**
   * delete columns
   * 
   * @param dd
   * @param table
   * @param columns
   */
  public static void deleteColumns(DatabaseData dd, String table, ColumnData[] columns) {
    String sql = "ALTER TABLE `" + dd.getDatabase() + "`.`" + table + "` ";
    for (int i=0; i < columns.length; i++) {
      String column = columns[i].getColumnName();
      sql += "DROP COLUMN `" + column + "`";
      if (i < columns.length-1) {
        sql += ", ";
      }
    }
    update(dd, sql);
  }

  /**
   * query column characters longest length,
   *    it looks through the entire columns recordset and finds the longest string's character count
   * @param dd
   * @param table
   * @param column
   * @return
   */
  public static int queryColumnCharactersLongestLength(DatabaseData dd, String table, ColumnData column) {
    String sql = "SELECT MAX(LENGTH(`" + column.getColumnName() + "`)) FROM " + dd.getDatabase() + "." + table + ";";
    return queryInteger(dd, sql);
  }

}
