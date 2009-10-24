package com.tribling.csv2sql.lib.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.data.DatabaseData;

public class MySqlReplication {

  // source database
  private DatabaseData dd_src = null;
  
  // destination database
  private DatabaseData dd_des = null;

  // tmp directory to store the dump, so to sync
  private File tmpDir;
  
  // master status
  private String statusFile = null;
  private String statusPosition = null;

  // replication user credentials
  private String replicationUsername = null;
  private String replicationPassword = null;
  private String masterHost = null;
  
  private int dryRun = 0;
  
  /**
   * constructor, init setup 
   * 
   * @param dd_src
   * @param dd_des
   * @param tmpDir
   */
  public MySqlReplication(DatabaseData dd_src, DatabaseData dd_des, File tmpDir, 
      String masterHost,  String replicationUsername, String replicationPassword) {
    this.dd_src = dd_src;
    this.dd_des = dd_des;
    this.tmpDir = tmpDir;
    this.replicationUsername = replicationUsername;
    this.replicationPassword = replicationPassword;
    this.masterHost = masterHost;
  }
  
  public void setDryRun() {
    dryRun = 1;
  }
  
  public void run() {
    
    // setup the replication user
    setupReplicationUser();
    
    // stop slave just in case doing it again
    stopSlave();
    
    // setup mysql.cnf on master
    setupMasterConf();
    
    // setup mysql.cnf on slave
    setupSlaveConf();
    
    // lock tables up so to get status
    lockTables();
    
    // get master's status
    getMasterStatus();
    
    // use this for syncing slave
    dumpMaster();
    
    // unlock master tables after dump
    unlockTables();
    
    // change to master
    setUpSlave();
    
    // load slave
    loadSlave();
    
    // start the slave threads
    startSlave();
    
    // delete tmp file at the end
    deleteTmpFile();
    
    System.out.println("Finished... Exiting...");
  }
  
  /**
   * 
   */
  private void setupReplicationUser() {
    
    // setup the replication user
    // TODO - if user is already created this will error
    String sqlCreateUser = "CREATE USER '" + replicationUsername + "'@'"+ dd_des.getHost() +"' IDENTIFIED BY '" + replicationPassword + "';";
    System.out.println(sqlCreateUser);
    MySqlQueryUtil.update(dd_src, sqlCreateUser);
    
    // setup replication user privileges
    String sqlCreateUserPerm = "GRANT REPLICATION SLAVE ON *.*  TO '" + replicationUsername + "'@'" + dd_des.getHost() + "' IDENTIFIED BY '" + replicationPassword + "';";
    System.out.println(sqlCreateUserPerm);
    MySqlQueryUtil.update(dd_src, sqlCreateUserPerm);
    
    // flush privileges;
    String sql = "FLUSH PRIVILEGES;";
    System.out.println(sql);
    MySqlQueryUtil.update(dd_src, sql);
    
    // TODO - maybe query the user to see if things are correct?
  }

  /**
   * setup master conf
   * 
   * TODO - automate later, manual setup for now
   */
  private void setupMasterConf() {
  
    // TODO - add to master my.cnf
    // [mysqld]
    // log-bin=mysql-bin
    // server-id=1
    
    int slaveId = getServerId(1);
    if (slaveId != 1) {
      System.out.println("master server_id variable in my.cnf not set correctly. Its set as on master server_id=" + slaveId);
      System.out.println("Exiting....");
      System.exit(1);
    } else {
      System.out.println("master serverid = 1, it passes.");
    }
    
  }
  
  /**
   * setup slave conf
   * 
   * TODO - automate later, manual setup for now
   */
  private void setupSlaveConf() {
    
    // TODO - add to slave my.cnf
    // server-id=2
    
    int slaveId = getServerId(2);
    if (slaveId != 2) {
      System.out.println("slave server_id variable in my.cnf not set correctly. Its set as on slave server_id=" + slaveId);
      System.out.println("Exiting....");
      System.exit(1);
    } else {
      System.out.println("slave server_id=2, it passes.");
    }
  }
  
  private void lockTables() {
    
    if (dryRun == 1) {
      System.out.println("Won't lock tables now. Skipping b/c of dry run.......");
      return;
    }
    
    String sql = "FLUSH TABLES WITH READ LOCK;";
    System.out.println(sql);
    MySqlQueryUtil.update(dd_src, sql);
  }
  
  private void unlockTables() {
    String sql = "UNLOCK TABLES;";
    System.out.println(sql);
    MySqlQueryUtil.update(dd_src, sql);
  }
  
  private void getMasterStatus() {
    
    String sql = "SHOW MASTER STATUS;";
    System.out.println(sql);
    try {
      Connection conn = dd_src.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        statusFile = result.getString(1);
        statusPosition = result.getString(2);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: Can't get master status: " + sql);
      e.printStackTrace();
    } 
  }
  
  private void dumpMaster() {
    String lock = "";
    if (dryRun != 1) {
      lock = "--lock-all-tables";
    }
    String cmd = "mysqldump -h" + dd_src.getHost() + " -u" + dd_src.getUsername() + " -p" + dd_src.getPassword() + " " + lock + " --all-databases > " + getTmpPath() + "/master_dump.sql";
    System.out.println(cmd);
    runShell(cmd);
  }
  
  /**
   * load slave with the dump from master
   * 
   * useing -f for passing on errors
   */
  private void loadSlave() {
    String cmd = "mysql -h" + dd_des.getHost() + " -u" + dd_des.getUsername() + " -p" + dd_des.getPassword() + " -f  < " + getTmpPath() + "/master_dump.sql";
    System.out.println(cmd);
    runShell(cmd);
  }
  
  private void setUpSlave() {
    
    String sql = "";
    sql += "CHANGE MASTER TO ";
    sql += "MASTER_HOST='" + masterHost + "', "; // master_host_name
    sql += "MASTER_USER='" + replicationUsername + "', "; // replication_user_name 
    sql += "MASTER_PASSWORD='" + replicationPassword + "', "; // replication_password
    sql += "MASTER_LOG_FILE='" + statusFile + "', "; // recorded_log_file_name
    sql += "MASTER_LOG_POS=" + statusPosition + "; "; // recorded_log_position
   
    System.out.println(sql);
    MySqlQueryUtil.update(dd_des, sql);
  }
  
  private void startSlave() {
    String sql = "START SLAVE;";
    System.out.println(sql);
    MySqlQueryUtil.update(dd_des, sql);
  }
  
  private void stopSlave() {
    String sql = "STOP SLAVE;";
    System.out.println(sql);
    MySqlQueryUtil.update(dd_des, sql);
  }
  
  private String getTmpPath() {
    String path = tmpDir.getPath();
    return path;
  }
  
  private int getServerId(int srcdes) {
    
    String sql = "SHOW VARIABLES like 'server_id'";
    System.out.println(sql);
    
    DatabaseData dd = null;
    if (srcdes == 1) {
      dd = dd_src; 
    } else {
      dd = dd_des;
    }
    
    String sid = null;
    try {
      Connection conn = dd.getConnection();
      Statement select = conn.createStatement();
      ResultSet result = select.executeQuery(sql);
      while (result.next()) {
        sid = result.getString(2);
      }
      select.close();
      select = null;
      result.close();
      result = null;
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error: Can't get master status: " + sql);
      e.printStackTrace();
    }
    return Integer.parseInt(sid);
  }
  
  /**
   * run shell command
   * 
   * @param cmd
   */
  private void runShell(String cmd) {
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
    pb.redirectErrorStream(true); 
    Process shell = null;
    try {
      shell = pb.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    InputStream shellIn = shell.getInputStream();                 
    try {
      int shellExitStatus = shell.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } 
    int c;
    try {
      while ((c = shellIn.read()) != -1) {
        System.out.write(c);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      shellIn.close();
    } catch (IOException ignoreMe) {
    }
  }

  private void deleteTmpFile() {
    System.out.println("deleting mysql dump file.");
    File tmpFile = new File(tmpDir.getPath() + "master_dump.sql");
    tmpFile.delete();
  }
  
}

