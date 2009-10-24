package com.tribling.csv2sql.test;

import java.io.File;

import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.lib.sql.MySqlReplication;

public class Run_Slave_Setup {

  public static void main(String[] args) {
    
    DatabaseData dd_src = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "user", "pass", "");
    DatabaseData dd_des = new DatabaseData(DatabaseData.TYPE_MYSQL, "sapphire", "3306", "user", "pass", "");
    
    File tmpDir = new File("/home/branflake2267/files/backup/mysql/replicate");
    
    String replicationUsername = "Replication";
    String replicationPassword = "ReplicationPass";
    
    MySqlReplication replicate = new MySqlReplication(dd_src, dd_des, tmpDir, replicationUsername, replicationPassword);
    replicate.setDryRun();
    replicate.run();
    
  }
  
}
