package org.gonevertical.csv2sql.lib.sql.transformlib;

import org.gonevertical.csv2sql.data.ColumnData;
import org.gonevertical.csv2sql.data.DatabaseData;

public interface TransformLib {

  public String fixTableName(String table);
  
  public void createTable(DatabaseData dd, String table, String primaryKeyName);

  public boolean doesTableExist(DatabaseData dd, String table);
  
  public boolean doesColumnExist(DatabaseData dd, ColumnData columnData);
  
  public ColumnData queryColumn(DatabaseData dd, String table, String columnName);
  
  public ColumnData queryColumn(DatabaseData dd, ColumnData columnData);
  
  public ColumnData[] queryColumns(DatabaseData dd, String table, String where);
  
  public ColumnData queryPrimaryKey(DatabaseData dd, String table);
  
  public boolean queryIsColumnPrimarykey(DatabaseData dd, ColumnData columnData);
  
  public ColumnData[] createColumn(DatabaseData dd, ColumnData[] columnData);
  
  public ColumnData createColumn(DatabaseData dd, ColumnData columnData);
  
  public void createColumn(DatabaseData dd, ColumnData column, int columnType, String length);
  
  public boolean doesIndexExist(DatabaseData dd, String table, String indexName);
  
  public void createIndex_forIdentities(DatabaseData dd, ColumnData[] columnData, String indexName);
  
  public void deleteColumn(DatabaseData dd, ColumnData columnData);
  
  public void deleteColumns(DatabaseData dd, ColumnData[] columnData);
  
  public void deleteEmptyColumns(DatabaseData dd, String table, ColumnData[] pruneColumnData);

  public long queryColumnCharactersLongestLength(DatabaseData dd, String table, ColumnData column);

  public String showCreateTable(DatabaseData dd, String table);
  
  public void dropTable(DatabaseData dd, String table);
  
  public boolean doesColumnContainData(DatabaseData dd, ColumnData columnData);
  
  public String[] deleteIndexForColumn(DatabaseData dd, ColumnData columnData);
  
  public String[] deleteIndexForColumn(DatabaseData dd, ColumnData[] columnData);
  
  public void deleteIndex(DatabaseData dd, String table, String indexName);
  
  public void alterColumn(DatabaseData dd, ColumnData columnData);
  
  public void alterColumn(DatabaseData dd, ColumnData[] columnData);
 
  public String[] showCreateIndex(DatabaseData dd, ColumnData columnData);
  
  
  
}
