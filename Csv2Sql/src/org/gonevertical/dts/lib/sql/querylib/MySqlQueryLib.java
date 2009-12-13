package org.gonevertical.dts.lib.sql.querylib;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringEscapeUtils;
import org.gonevertical.dts.data.DatabaseData;


public class MySqlQueryLib implements QueryLib {

  public MySqlQueryLib() {
  }
  
  /**
   * escape string
   * 
   * fixes: Strings can come like this abcdefg\  and then when made into a statement 'abcdefg\'
   * I used to trim here, but not a good idea b/c I want to replicate exactly what comes in.
   *  
   * @param s
   * @return
   */
  public String escape(String s) {
    if (s == null) {
      s = "";
    }
    // escape quotes
    s = StringEscapeUtils.escapeSql(s);
    
    // TODO - maybe I only need it at end of string????
    // when string looks like this column='value\' or column='value\\'
    //if (s.matches(".*[\\]") == true) {
    // TODO this needs to be fixed b/c it will send mess up \t\r\n
      s = s.replaceAll("\\\\", "\\\\\\\\");
    //}
    return s;
  }
  
  public String escape(int i) {
    return escape(Integer.toString(i));
  }
  
  public int getResultSetSize(ResultSet result) {
    int size = -1;
    try {
      result.last();
      size = result.getRow();
      result.beforeFirst();
    } catch (SQLException e) {
      System.err.println("Error: getResultSetSize()");
      e.printStackTrace();
    } 
    return size;
  }
  
  public boolean queryBoolean(DatabaseData dd, String sql) {
    boolean b = false;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        b = result.getBoolean(1);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryBoolean(): " + sql);
      e.printStackTrace();
    } 
    return b;
  }

  public int queryInteger(DatabaseData dd, String sql) {
    int i = 0;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        i = result.getInt(1);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryInteger(): " + sql);
      e.printStackTrace();
    } 
    return i;
  }
  
  public long queryLong(DatabaseData dd, String sql) {
    long i = 0;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        i = result.getLong(1);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryInteger(): " + sql);
      e.printStackTrace();
    } 
    return i;
  }

  public String queryString(DatabaseData dd, String sql) {
    String s = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        s = result.getString(1);
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

  public double queryDouble(DatabaseData dd, String sql) {
    double d = 0.0;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        d = result.getDouble(1);
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryDouble(): " + sql);
      e.printStackTrace();
    } 
    return d;
  }

  public BigDecimal queryBigDecimal(DatabaseData dd, String sql) {
    BigDecimal bd = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        if (result.getString(1) != null) {
          bd = new BigDecimal(result.getString(1));
        } 
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryBigDecimal(): " + sql);
      e.printStackTrace();
    } 
    return bd;
  }
  
  public String queryIntegersToCsv(DatabaseData dd, String sql, char delimiter) {
    String csv = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      int size = getResultSetSize(result);
      int i = 0;
      if (size > 0) {
        csv = "";
      }
      while (result.next()) {
        int id = result.getInt(1);
        String sid = Integer.toString(id);
        csv += sid;
        if (i < size-1) { 
          csv += Character.toString(delimiter);
        }
        i++;
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryIntegersToCsv(): " + sql);
      e.printStackTrace();
    } 
    if (csv == null | csv.length() == 0) {
      csv = "NULL";
    }
    return csv;
  }
  
  public String queryStringToCsv(DatabaseData dd, String sql, char delimiter) {
    
    String csv = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      int size = getResultSetSize(result);
      if (size > 0) {
        csv = "";
      }
      int i = 0;
      while (result.next()) {
        String v = result.getString(1);
        csv += "\"" + v + "\"";
        if (i < size-1) { 
          csv += Character.toString(delimiter);
        }
        i++;
      }
      result.close();
      result = null;
      select.close();
      select = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: queryStringToCsv(): " + sql);
      e.printStackTrace();
    } 
    if (csv == null | csv.length() == 0) {
      csv = "NULL";
    }
    return csv;
  }

  public long update(DatabaseData dd, String sql) {
    if (sql == null) {
      return 0;
    }
    long id = 0;
    try {
      Connection conn = dd.getConnection();
      if (dd.getLoadBalance() == true) {
        conn.setReadOnly(false);
      }
      Statement update = conn.createStatement();
      update.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
      ResultSet result = update.getGeneratedKeys();
      if (result != null && result.next()) { 
        id = result.getLong(1);
      }
      result.close();
      result = null;
      update.close();
      update = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: update(): " + sql);
      e.printStackTrace();
    } 
    return id;
  }
  
  public long update(DatabaseData dd, String sql, boolean getKey) {
    if (sql == null) {
      return 0;
    }
    long id = 0;
    try {
      Connection conn = dd.getConnection();
      if (dd.getLoadBalance() == true) {
        conn.setReadOnly(false);
      }
      Statement update = conn.createStatement();
      update.executeUpdate(sql);

      if (getKey == true) {
        ResultSet result = update.getGeneratedKeys();
        if (result != null && result.next()) { 
            id = result.getLong(1);
          }
          result.close();
          result = null;
      }
      update.close();
      update = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: update(): " + sql);
      e.printStackTrace();
    } 
    return id;
  }
  
  public boolean queryStringAndConvertToBoolean(DatabaseData dd, String sql) {
    String value = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        value = result.getString(1);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    } 
    boolean b = false;
    if ((value != null && value.length() > 0) && 
        value.equals("0") == false | 
        value.toLowerCase().equals("false") == false) {
      b = true;
    }
    return b;
  }
  
  public boolean queryLongAndConvertToBoolean(DatabaseData dd, String sql) {
    long l = 0;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        l = result.getLong(1);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Mysql Statement Error:" + sql);
      e.printStackTrace();
    } 
    boolean b = false;
    if (l > 0) {
      b = true;
    }
    return b;
  }

  public String getType() {
    return "MySql";
  }

  
}
