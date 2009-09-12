package com.tribling.csv2sql.v2;

import java.util.ArrayList;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformAlterUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class Indexing {

  private DestinationData_v2 destinationData = null;
  
  private ColumnData[] columns = null;
  
  private ArrayList<String> index = new ArrayList<String>();

  // full text indexing
  private boolean fullTextIndex;
  
  public Indexing(DestinationData_v2 destinationData) {
    this.destinationData = destinationData;
  }
  
  public void runIndexColumns(ColumnData[] indexColumns) {
    if (indexColumns == null) {
      return;
    }
    this.columns = indexColumns;  
    indexColumns();
    
    index();
  }
  
  public void setFullText(boolean b) {
    this.fullTextIndex = b;
  }
  
  private void indexColumns() {
    for (int i=0; i < columns.length; i++) {
      if (columns[i]== null) {
        // skip
      } else if (columns[i].getColumnName().matches("Auto_.*") == true) {
        // skip
      } else {
        String in = getIndex(i, columns[i]);
        if (in != null) {
          index.add(in);
        }
      }
    }
  }
  
  private String getIndex(int i, ColumnData columnData) {
    
    String nm = "";
    if (columnData.getColumnName().length() < 4) {
      nm = columnData.getColumnName();
    } else {
      nm = columnData.getColumnName().substring(0,4);
    }
    
    String cn = columnData.getColumnName();
    String indexName = "`auto_" + nm + "_"+ i + "`";
    
    // does the index already exist?
    boolean exists = MySqlTransformUtil.doesIndexExist(destinationData.databaseData, columnData.getTable(), indexName);
    if (exists == true) {
      return null;
    }

    String len = "";
    if (columnData.getType().contains("text") == true) {
      len = "(900)";
    } 
    
    String kind = "";
    if (fullTextIndex == true) {
      kind = "FULLTEXT";
      len = "";
    }
    String sql = "ADD " + kind + " INDEX " + indexName + "(`" + cn + "`" + len + ")";
    
    return sql;
  }
  
  private void index() {
    
    if (index.size() == 0) {
      return;
    }
    String table = columns[0].getTable();
    String sql = "ALTER TABLE " + table + " ";
    for (int i=0; i < index.size(); i++) {
      sql += index.get(i); 
      if (i < index.size() - 1) {
        sql += ",";
      }
    }
    
    System.out.println("index: " + sql);
    MySqlQueryUtil.update(destinationData.databaseData, sql);
  }
}
