package com.tribling.csv2sql.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.lib.FileUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class Export {
  
  public static final int EXPORTAS_CSV = 1;
  public static final int EXPORTAS_SQL = 2;
  
  private int exportAs = EXPORTAS_CSV;
  
  private DatabaseData src = null;
  
  // destination file
  private File des = null;
  
  // exporting to this file
  private File file = null;
  
  // keep track of what output file export is on
  // des.csv|sql
  // des_1.csv|sql
  private int fileIndex = 0;
  
  // columns that will be exported
  private ColumnData[] columnData = null;
  
  // table to export data from
  private String table = null;
  
  // filter query using where
  private String whereSql = null;
  
  // like LIMIT 0,100
  private String limitSql = null;

  // output to file
  private BufferedWriter out = null;
  
  // if exporting as SQL - do you want to create table script in the export
  private boolean createTable;

  // keep files around <= 1GB
  private long maxOutFileSizeKB = 1048576;
  
  // skip these columns
  private ColumnData[] pruneColumnData = null;
  
  public Export(DatabaseData source, File destinationDirectory) {
    this.src = source;
    this.des = destinationDirectory;
  }
  
  /**
   * which table would you like to export from?
   * 
   * @param table
   */
  public void setTable(String table) {
    this.table = table;
  }
  
  public void setTable(String table, String whereSql, String limitSql) {
    this.table = table;
    this.whereSql = whereSql;
    this.limitSql = limitSql;
  }
  
  /**
   * which columns would you like to export?
   *   if null will export all
   * 
   * @param columns
   */
  public void setColumns(ColumnData[] columns) {
    this.columnData = columns;
  }
  
  /**
   * if exporting as sql, do you want to create table script in it?
   * 
   * @param b
   */
  public void setShowCreateTable(boolean b) {
    this.createTable = b;
  }
  
  /**
   * prune these columns out
   * 
   * @param pruneColumnData
   */
  public void setPruneColumns(ColumnData[] pruneColumnData) {
    this.pruneColumnData = pruneColumnData;
  }
  
  /**
   * export as csv or sql
   * 
   * @param exportAs static constants in this file
   */
  public void run(int exportAs) {
    this.exportAs = exportAs;
    
    if (des == null) {
      System.out.println("no destination file set");
      return;
    }
    
    // get all columns?
    if (columnData == null) {
      columnData = getColumns();
    }
    
    // prune columns?
    if (pruneColumnData != null) {
      columnData = ColumnData.prune(columnData, pruneColumnData);
    }
    
    setFile();
    
    openFile();
    
    loopData();
    
    closeFile();
    
    System.out.println("Finished!");
  }
  
  /**
   * start exporting the data
   */
  private void loopData() {
    
    String sql = "SELECT ";
    sql += ColumnData.getSql_Names(columnData) + " ";
    sql += "FROM `" + table + "` ";
    
    if (whereSql != null) {
      sql += " WHERE " + whereSql;
    }
    
    if (limitSql != null) {
      sql += " " + limitSql;
    }
    
    try {
      Connection conn = src.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      int i = 0;
      while (result.next()) {
        columnData = ColumnData.getResult(result, columnData);
        appendToFile(i);
        i++;
      }
      result.close();
      result = null;
      select.close();
      select = null;
    } catch (SQLException e) {
      System.err.println("Error: loopData(): " + sql);
      e.printStackTrace();
    }
    
  }
  
  /**
   * write the data to the file
   * 
   * @param i - row index
   */
  private void appendToFile(int i) {
    
    String s = "";
    if (i == 0) {
      s = getHeader();
      s += getRows();
    } else {
      s = getRows();
    }
    
    // keep files <= 1GB
    checkFileSize();
    
    // DEBUG
    System.out.println("DEBUG OUT: " + s);
    
    writeFile(s);
  }
  
  /**
   * get header values
   * 
   * @return
   */
  private String getHeader() {
    String s = "";
    if (exportAs == EXPORTAS_CSV) {
      s = ColumnData.getCsv_Names(columnData);
    } else if (exportAs == EXPORTAS_SQL) {
      s = showTableCreate() + "\n\n";
    }
    s += "\n";
    return s;
  }
  
  /**
   * show create table syntax
   * @return
   */
  private String showTableCreate() {
    String s = "";
    if (createTable == true) {
      s = MySqlTransformUtil.showCreateTable(src, table);
    }
    return s;
  }
  
  /**
   * get row values
   * 
   * @return
   */
  private String getRows() {
    String s = "";
    if (exportAs == EXPORTAS_CSV) {
      s = ColumnData.getCsv_Values(columnData);
    } else if (exportAs == EXPORTAS_SQL) {
      s = ColumnData.getSql_Insert(columnData);
    }
    s += "\n";
    return s;
  }
  
  /**
   * get all columns of table
   * 
   * @return
   */
  private ColumnData[] getColumns() {
    ColumnData[] c = MySqlTransformUtil.queryColumns(src, table, null);
    return c;
  }
  
  /**
   * open the file for writing to
   */
  private void openFile() {
    try {
      out = new BufferedWriter(new FileWriter(file, false));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * write to file
   * 
   * @param s
   */
  private void writeFile(String s) {
    try {
      out.write(s);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * close file
   */
  private void closeFile() {
    try {
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * set the file name to export to
   * 
   * @param index
   */
  private void setFile() {
    String ext = "";
    if (exportAs == EXPORTAS_CSV) {
      ext = "csv";
    } else if (exportAs == EXPORTAS_SQL) {
      ext = "sql";
    }
    String fileName = des.getAbsolutePath() + "/" + table + "_" + fileIndex + "." + ext;
    file = new File(fileName);
  }
  
  /**
   * check the file size is less than 1GB
   */
  private void checkFileSize() {
    long sizeinkb = FileUtil.getFileSize(file); 
    if (sizeinkb >= maxOutFileSizeKB) {
      fileIndex++;
      closeFile();
      setFile();
      openFile();
    }
  }
  
  
}
