package org.gonevertical.dts.lib.experimental.install;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.gonevertical.dts.lib.FileUtil;

/**
 * set a second instance up of mysql on ubuntu
 * 
 * @author branflake2267
 *
 */
public class MySqlSecond {

  private FileUtil fu = new FileUtil();
  
  private int instanceNumber = 2;
  
  public MySqlSecond() {
    
  }
  
  public void run() {
    
    // TODO - is there a first instance?
    // TODO - is there a mysql user? I suppose so since its instance #2
    
    // FILE: etc/mysql/*
    copyConfFiles();
    
    // FILE: /etc/init.d/mysql
    copyServiceFile();
    
    // setup new my.cnf attributes
    setMyCnf2();
    
    // setup new debian attributes
    setDebianCnf2();
    
    // setup service
    setInitService();
    
    // TODO set apparmor
    
    // TODO setup debian maintance user from FILE: /etc/init.d/mysql2/debian.cnf - have to run as root
    
    // 
    
    // rc.d - set /etc/init.d/mysql2 to load on boot
  }
  
  /**
   * set port, socket, datadir in /etc/init.d/mysql2/*
   * in: /etc/init.d/mysql2
   * FILE: /etc/init.d/mysql2/my.cnf
   */
  private void setMyCnf2() {
    

    // TODO set port
    // TODO set socket
    // TODO set datadir
    // TODO set pid-file
    // TODO log_bin
    
    // regex entire properties file /etc/init.d/mysql to /etc/init.d/mysql
    fu.replaceInFileByLine(new File("/etc/mysql"+instanceNumber+"/my.cnf"), "/mysql", "/mysql"+instanceNumber);
  }
  
  /**
   * setup the debain start attributes
   * 
   * FILE: /etc/mysql/debian-start.cnf
   */
  private void setDebianCnf2() {
    // FILE: /etc/init.d/mysql2/debian-start
    
    // TODO set socket from /var/run/mysqld/mysqld.sock to /var/run/mysqld/mysqld2.sock
    // TODO set regex /etc/mysql to /etc/mysql2 
  }
  
  /**
   * set service start properties
   * 
   *  FILE: /etc/init.d/mysql2
   */
  private void setInitService() {
    // TODO set CONF=/etc/mysql/my.cnf to CONF=/etc/mysql2/my.cnf
    // TODO set MYADMIN="/usr/bin/mysqladmin --defaults-file=/etc/mysql2/debian.cnf" 
    
    // set by regex: /etc/mysql to /etc/mysql2
    // set by regex: /etc/init.d/mysql to /etc/init.d/mysql2
    fu.replaceInFileByLine(new File("/etc/init.d/mysql"+instanceNumber), "/mysql", "/mysql"+instanceNumber);
    
  }
  
  private void copyConfFiles() {
    String command = "cp -R /etc/mysql /etc/mysql" + instanceNumber;
    try {
      Runtime.getRuntime().exec(command);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void copyServiceFile() {
    File src = new File("/etc/init.d/mysql");
    File dst = new File("/etc/intid.d/mysql" + instanceNumber);
    copyFile(src, dst);
    
    src = new File("/etc/init.d/mysql-ndb");
    dst = new File("/etc/intid.d/mysql-ndb" + instanceNumber);
    copyFile(src, dst);
    
    src = new File("/etc/init.d/mysql-ndb-mgm");
    dst = new File("/etc/intid.d/mysql-ndb-mgm" + instanceNumber);
    copyFile(src, dst);
  }
   
  private void copyFile(File src, File dst) {
    InputStream in = null;
    try {
      in = new FileInputStream(src);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    OutputStream out = null;
    try {
      out = new FileOutputStream(dst);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    // Transfer bytes from in to out
    byte[] buf = new byte[1024];
    int len;
    try {
      while ((len = in.read(buf)) > 0) {
          out.write(buf, 0, len);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void changeProperty(File file, String property, String value) {
    
    PropertiesConfiguration config = null;
    try {
      config = new PropertiesConfiguration(file);
    } catch (ConfigurationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    config.setProperty(property, value);
    try {
      config.save();
    } catch (ConfigurationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    
  }
  
}
