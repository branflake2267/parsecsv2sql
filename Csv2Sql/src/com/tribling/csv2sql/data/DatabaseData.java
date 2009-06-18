package com.tribling.csv2sql.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseData {

  public static final int TYPE_MYSQL = 1;
  public static final int TYPE_MSSQL = 2;
  public static final int TYPE_JDO = 3;
  
  // location
  private String host;
  private String port;
  
  // credentials
  private String username;
  private String password;
  
  // database
  private String database;
  
  // what type of database is it? Mysql, Jdo, ...
  private int databaseType = 0;
  
  // setup a connection and store it in this object for easy reference to
  private Connection conn = null;
  
  /**
   * set database location and credentials
   * 
   * @param host
   * @param port
   * @param username
   * @param password
   * @param database
   */
  public DatabaseData(int databaseType, String host, String port, String username, String password, String database) {
    this.databaseType = databaseType;
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.database = database;
  }
  
  public int getDatabaseType() {
    return databaseType;
  }
  
  public String getHost() {
    return host;
  }
  
  public String getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }
  
  public String getPassword() {
    return password;
  }
  
  public String getDatabase() {
    return database;
  }

  public void openConnection() {
    if (databaseType == TYPE_MYSQL) {
      conn = getConn_MySql();
    } else if (databaseType == TYPE_MSSQL) {
      conn = getConn_MsSql();
    } 
    // TODO - add jdo?
  }
  
  public Connection getConnection() {
    return conn;
  }
  
  public void closeConnection() {
    try {
      conn.close();
    } catch (SQLException e) {
    }
  }
  
  /**
   * get a mysql database connection
   * 
   * @return
   */
  private Connection getConn_MySql() {

    String url = "jdbc:mysql://" + host + ":" + port + "/";
    String driver = "com.mysql.jdbc.Driver";
    //System.out.println("getConn_MySql: url:" + url + " user: " + username + " driver: " + driver);

    try {
      Class.forName(driver).newInstance();
      conn = DriverManager.getConnection(url + database, username, password);
    } catch (Exception e) {
      System.err.println("MySql Connection Error:");
      e.printStackTrace();
      System.out.println("Fix Connection.");
      System.exit(1);
    }

    return conn;
  }
  
  /**
   * get ms sql connection
   * 
   * Driver is different for 2000 vs 2005, this may change
   * 
   * @return
   */
  private Connection getConn_MsSql() {

    String url = "jdbc:sqlserver://" + host + ";user=" + username + ";password=" + password + ";databaseName=" + database + ";";
    String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    //System.out.println("getConn_MsSql: url:" + url + " user: " + username + " driver: " + driver);

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
}
