package com.tribling.csv2sql.v2;


/**
 * sql processing
 * 
 * @author branflake2267
 * 
 */
public class SQLProcessing_v2 {
 

  
  /*
   
  private void dealWithSqlError(SQLException e, String query) {
    
    if (dealwithTruncationError(e, query) == true) {
      
    } else {
      System.err.println("Mysql Statement Error: " + query);
      e.printStackTrace();
      System.out.println("");
    }

  }
  
  private boolean dealwithTruncationError(SQLException e, String query) {
    
    // mysql truncation error -> Data truncation: Data too long for column 'Period' at row 1
    
    String msg = e.getMessage();
    
    // TODO - datetime date 01/01/2009 being put into a datetime column
    if (msg.contains("too long") == false) { // || msg.contains("truncation") == false happens whith datetime stuffing
      return false;
    } 
    
    // get column
    String column = getTruncationColumn(msg);
    if (column == null) {
      return false;
    }
    
    // get value length needed
    int length = getTruncationValueLength(column, query);
    if (length == 0) {
      return false;
    }
    
    // get column type
    ColumnData cd = getColumn(column);
    String columnType = cd.columnType;
    
    resizeColumnLength(column, columnType, length);
    
    // TODO - deal with recursion error here
    updateSql(query);
    
    return true;
  }
  
  private String getTruncationColumn(String s) {
    
    String re = "\'(.*?)\'";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(s);
    boolean found = m.find();
    
    String f = null;
    if (found == true) {
      f = m.group(1);
    } 
    
    return f;
  }
  
  private int getTruncationValueLength(String column, String query) {
    
    // UPDATE db.table SET `column`='01/01/2008' WHERE ImportId='1';
    
    String re = "SET.*?`" + column + "`?.*?=.*?\'(.*?)\'";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(query);
    boolean found = m.find();
    
    String f = "";
    if (found == true) {
      f = m.group(1);
    } 
    
    int i = f.length();
    
    return i;
  }


  private void createIndex_forIdentities() {

    if (dd.identityColumns == null) {
      System.out.println("skipping creating indexes, b/c there are no identiy columns listed");
      return;
    }

    String indexes = "";
    for (int i = 0; i < dd.identityColumns.length; i++) {
      
      String column = dd.identityColumns[i].destinationField;
      
      // force a columnType if need be
      String columnType = null;
      if (dd.identityColumns[i].destinationField_ColumnType == null) {
        columnType = "VARCHAR(50) DEFAULT NULL";
      } else {
        columnType = dd.identityColumns[i].destinationField_ColumnType;
      }
      createColumn(column, columnType);

      if (databaseType == 1) {
        indexes += "`" + column + "`";
        
      } else if (databaseType == 2) {
        indexes += "[" + column + "]";
      }

      if (i < dd.identityColumns.length - 1) {
        indexes += ", ";
      }
    }
    
    // created the index
    String indexName = "index_auto";
    createIndex(indexName, indexes, Optimise_v2.INDEXKIND_DEFAULT);
  }


  protected void createIndex(String indexName, String columns, int indexKind) {

    // TODO not sure of actual length limitation for indexName
    if (indexName.length() > 30) {
      indexName = indexName.substring(0,30);
    }
    
    if (doesIndexExist(indexName) == true) {
      return;
    }
    
    // TODO - do for MS too
    String kind = "";
    if (indexKind == Optimise_v2.INDEXKIND_FULLTEXT) {
      kind = "FULLTEXT";
    }
    
    // TODO if the column is a text column, set the index length and its not full text

    String query = "";
    if (databaseType == 1) {
      query = "ALTER TABLE `" + dd.database + "`.`" + dd.table + "` "
          + "ADD " + kind + " INDEX `" + indexName + "`(" + columns + ");";
      
    } else if (databaseType == 2) {
      query = "CREATE INDEX [" + indexName + "] ON " + dd.database + "."
          + dd.tableSchema + "." + dd.table + " (" + columns + ") ;";
    }

    updateSql(query);
  }



  protected void deleteAllIndexs() {

    String sql = "";
    if (databaseType == 1) {
      sql = "SHOW INDEX FROM `" + dd.table + "` FROM `" + dd.database + "` " + 
        "WHERE Key_name != 'Primary'";
    } else if (databaseType == 2) {
      sql = "";
    }
    
    System.out.println("sql: " + sql);
    
    try {
      Connection conn = getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      
      while (result.next()) {
        // TODO - Key_name is 3 for mysql , don't know for Mssql yet
        String index = result.getString(3);
        deleteIndex(index);
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    

  }

 




 



  private int getRecordExist(ColumnData[] columns, String[] values) {

    int id = 0;
    if (databaseType == 1) {
      id = getRecordExist_MySql(columns, values);
    } else if (databaseType == 2) {
      id = getRecordExist_MsSql(columns, values);
    }

    return id;
  }

  private int getRecordExist_MySql(ColumnData[] columns, String[] values) {

    // get idents
    String whereQuery = getIdentiesWhereQuery(columns, values);

    if (whereQuery.length() == 0) {
      return -1;
    }

    String query = "SELECT ImportId FROM `" + dd.database + "`.`" + dd.table
        + "` " + "WHERE " + whereQuery + " LIMIT 0,1";

    System.out.println("Exist?: " + query);
    
    int id = getQueryIdent(query);

    return id;
  }

  private int getRecordExist_MsSql(ColumnData[] columns, String[] values) {

    // get idents
    String whereQuery = getIdentiesWhereQuery(columns, values);

    if (whereQuery.length() == 0) {
      return -1;
    }

    String query = "SELECT TOP 1 ImportId FROM " + dd.database + "."
        + dd.tableSchema + "." + dd.table + " " + "WHERE " + whereQuery;

    int id = getQueryIdent(query);

    return id;
  }

  private String getQuery_Update_MySql(ColumnData[] columns, String[] values,
      int id) {

    String q = "";
    for (int i = 0; i < columns.length; i++) {

      String c = "";
      String v = "";

      c = columns[i].column;

      try {
        v = values[i];
      } catch (Exception e) {
        v = "";
      }

      v = escapeForSql(v);

      if (c.length() > 0) {
        q += "`" + c + "`='" + v + "'";

        if (i < columns.length - 1) {
          q += ", ";
        }
      }
    }

    q = fixcomma(q);

    String s = "UPDATE `" + dd.database + "`.`" + dd.table + "` "
        + "SET DateUpdated=NOW(), " + q + " " + "WHERE (ImportID='" + id
        + "');";

    return s;
  }

  private void doDataLengthsfit(String[] values) {

    if (values == null) {
      return;
    }

    int resize = 0;
    for (int i = 0; i < columns.length; i++) {

      String value = "";
      try {
        value = values[i];
      } catch (Exception e) {
      }
      resize = columns[i].testValue(value);
      if (resize > 0) {
        String type = resizeColumnLength(columns[i].column, columns[i].columnType, resize);
        columns[i].setType(type);
      }
    }
  }

  private String resizeColumnLength(String column, String columnType, int length) {

    if (length == 0) {
      return "";
    }

    Optimise_v2 optimise = new Optimise_v2();
    optimise.setDestinationData(dd);
    optimise.setMatchFields(matchFields);
    optimise.openConnection();
    String r = optimise.resizeColumn(column, columnType, length);
    optimise.closeConnection();
    return r;
    
    return null;
  }

  protected ColumnData[] createReverseColumns(ColumnData[] columns) {
    
    ColumnData[] rtn = new ColumnData[columns.length];
    for (int i=0; i < columns.length; i++) {
      rtn[i] = new ColumnData();
      rtn[i] = createReversedColumn(columns[i]);
    }
    
    return rtn;
  }

  private ColumnData createReversedColumn(ColumnData c) {
    
    String srcColumn = c.column;
    String dstColumn = c.column + "__Reverse";
    String type = c.columnType;
    createColumn(dstColumn, type);
    
    // copy the data
    String sql = "";
    if (databaseType == 1) {
      sql = "UPDATE " + dd.table + " " +
        "SET " + dstColumn + "=REVERSE(" + srcColumn + ") " +
        "WHERE (" + dstColumn + "='');"; // TODO - (dstColumn IS NOT NULL);
    } else if (databaseType == 2) {
      // TODO finish later
      sql = "";
    }
    
    System.out.println("copying reverse: " + sql);
    
    updateSql(sql);
    
    System.out.println("finished with reverse copy");
    
    // send back reverse column name for indexing
    ColumnData rtn = c;
    rtn.column = dstColumn;
    return rtn;
  }
  
  protected int getTableHasDuplicates() {
    
    // get total record count for table
    int tc = getTableRecordCount();
    
    // check distinct count for identities
    int tdc = getTableDistinctIdentCount();
    
    int r = tc - tdc;
    
    return r;
  }

  private int getTableRecordCount() {
    String sql = "";
    if (databaseType == 1) {
      sql = "SELECT COUNT(*) AS t FROM " + dd.database + "." + dd.table + ";"; 
    } else if (databaseType == 2) {
      // TODO
      sql = "";
    }
    return getQueryInt(sql);
  }
  
  private int getTableDistinctIdentCount() {
    
    if (dd.identityColumns == null) {
      return 0;
    }
    
    // get ident columns
    String idents_Columns = getIdentitiesColumns_inCsv();
    
    String sql = "";
    if (databaseType == 1) {
      sql = "SELECT DISTINCT " + idents_Columns + " FROM " + dd.database + "." + dd.table + ";"; 
    } else if (databaseType == 2) {
      // TODO
      sql = "";
    }

    int c = 0;
    try {
      Connection conn = getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      
      if (databaseType == 1) {
        c = getResultSetSize(result);
      } else if (databaseType == 2) {
        while (result.next()) {
          c++;
        }
      }
      select.close();
      result.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    }
    
    return c;
  }
  */
}
