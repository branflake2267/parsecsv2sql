package com.tribling.csv2sql.process;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.lib.sql.MySqlQueryUtil;
import com.tribling.csv2sql.lib.sql.MySqlTransformUtil;

public class ImportSqlScript {

  public DatabaseData dd = null;
  
  public File file = null;
  
  public ImportSqlScript(DatabaseData destination) {
    dd = destination;
  }
  
  public void run(File file) {
    this.file = file;
    
    readFile();
  }
  
  private void readFile() {
    try{
      FileInputStream fstream = new FileInputStream(file);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String strLine;
      while ((strLine = br.readLine()) != null)   {
        process(strLine);
      }
      in.close();
    }catch (Exception e){
      System.err.println("Error: " + e.getMessage());
    }
  }

  private void process(String strLine) {
  
    // TODO should I use mysql -u.... or find a jdbc class/method?
    
  }
  
}
