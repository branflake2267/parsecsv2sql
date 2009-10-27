package com.tribling.csv2sql.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

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
  //private Connection conn = null;
  
  // servlet context - used for connection pooling
  private Context context = null;
  
  // data connection pool resource name
  // like "jdbc/TestDB" - needs to be in web.xml and server.xml
  private String contextRefName = null;
  
  // server info from servlet thread - getServletContext().getServerInfo();
  // to use this connection pooling servlet needs to be running on tomcat
  private String serverInfo = null;
  
  // load balance master, slave, slave...
  private boolean autoReconnect = false;
  private boolean roundRobinLoadBalance = false;
  private boolean readOnly = false;
  
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
  
  /**
   * http://dev.mysql.com/doc/refman/5.4/en/connector-j-reference-configuration-properties.html
   */
  public void setLoadBalance(boolean b) {
    if (b == true) {
      autoReconnect = true;
      roundRobinLoadBalance = true;
    } else {
      autoReconnect = true;
      roundRobinLoadBalance = false;
    }
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
    
  /**
   * open connection
   * @return
   */
  public Connection openConnection() {
  
    Connection conn = null;
    
    if (context != null && contextRefName != null && serverInfo.toLowerCase().contains("tomcat")) {
      conn = getServletConnetion();
    } else {
      if (databaseType == TYPE_MYSQL) {
        conn = getConn_MySql();
      } else if (databaseType == TYPE_MSSQL) {
        conn = getConn_MsSql();
      } 
    }

    return conn;
  }
  
  /**
   * same as open connection
   * @return
   */
  public Connection getConnection() {
    return openConnection();
  } 
    
  /**
   * close the database connection
   *  Don't really need to use this.
   *  conn.close();
   */
  public void closeConnection() {
    //if (conn == null) {
      //return;
    //}
    
    //try {
      //conn.close();
    //} catch (SQLException e) {
      //e.printStackTrace();
    //} finally {
      //conn = null;
    //}
  }
  
  /**
   * get a mysql database connection
   * 
   * jdbc:mysql://[host][,failoverhost...][:port]/[database] » [?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...
   * 
   * parameters list
   * http://dev.mysql.com/doc/refman/5.4/en/connector-j-reference-configuration-properties.html
   * 
   * @return
   */
  private Connection getConn_MySql() {
    Connection conn = null;
    
    String loadBalance = "";
    if (roundRobinLoadBalance == true) {
      loadBalance = "?roundRobinLoadBalance=true&autoReconnect=true";
    }
    
    String url = "jdbc:mysql://" + host + ":" + port + "/" + database + loadBalance;
    String driver = "com.mysql.jdbc.Driver";
    //System.out.println("getConn_MySql: url:" + url + " user: " + username + " driver: " + driver);

    if (roundRobinLoadBalance == true) {
      driver = "com.mysql.jdbc.ReplicationDriver";
    }
    
    try {
      Class.forName(driver).newInstance();
      conn = DriverManager.getConnection(url, username, password);
    } catch (Exception e) {
      System.err.println("ERROR: getConn_MySql(): connection error: " + e.getMessage() + " " + "getConn_MySql: url:" + url + " user: " + username + " driver: " + driver);
      e.printStackTrace();
    }
    
    if (roundRobinLoadBalance == true) {
      try {
        conn.setReadOnly(true);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    
    if (readOnly == true) {
      try {
        conn.setReadOnly(readOnly);
      } catch (SQLException e) {
      }
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
    Connection conn = null;
    
    String url = "jdbc:sqlserver://" + host + ";user=" + username + ";password=" + password + ";databaseName=" + database + ";";
    String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    //System.out.println("getConn_MsSql: url:" + url + " user: " + username + " driver: " + driver);

    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception e) {
      System.err.println("ERROR: getConn_MsSql(): connection error: " + e.getMessage());
      e.printStackTrace();
    }
    return conn;
  }
  
  /**
   * set initial context for servlet connection pooling 
   * 
   * @param serverInfo String serverInfo = getServletContext().getServerInfo();
   * @param context - inital jndi context
   * @param contextRefName - resource name to connection pool connection like "jdbc/mydbresname"
   *    this must be in server.xml and web.xml 
   */
  public void setServletContext(String serverInfo, Context context, String contextRefName) {
    this.serverInfo = serverInfo;
    this.context = context;
    this.contextRefName = contextRefName;
  }
  
  /**
   * get servlet connection for tomcat6 connection pooling 
   * 
   *  make sure /usr/share/tomcat6/lib has jdbc driver
   * 
   * @return
   */
  public Connection getServletConnetion() {
  
    if (context == null) {
      System.out.println("ERROR: getServletConnetion(): no context set");
      return null;
    }
  
    if (contextRefName == null) {
      System.out.println("ERROR: getServletConnetion(): no contextRefName set");
      return null;
    }
    
    // first get a datasource
    DataSource ds = null;
    try {
      ds = (DataSource) context.lookup(contextRefName); 
    } catch (NamingException e) {
      System.out.println("ERROR: getServletConnetion(): NO datasource");
      e.printStackTrace();
    }
    
    // use the datasource to get the connection
    Connection conn = null;
    try {
      conn = ds.getConnection();
    } catch (SQLException e) {
      System.out.println("ERROR: getServletConnetion(): couldn't get servlet connection");
      e.printStackTrace();
    }
    
    return conn;
  }
  
  /**
   * init a context for use with servlet connection - don't do this over and over
   * 
   * @return
   */
  public static Context initContext() {

    Context initContext = null;
    try {
      initContext = new InitialContext();
    } catch (NamingException e) {
      System.out.println("ERROR: initContext(): Could not get InitalContext");
      e.printStackTrace();
    }
  
    Context ctx = null;
    try {
      ctx = (Context) initContext.lookup("java:/comp/env");
    } catch (NamingException e) {
      System.out.println("ERROR: initContext(): Could not init Context");
      e.printStackTrace();
    }

    return ctx;
  }
  
  public boolean getLoadBalance() {
    return this.roundRobinLoadBalance;
  }

  public void setReadOnly(boolean b) {
    this.readOnly  = b;
  }
  
}
