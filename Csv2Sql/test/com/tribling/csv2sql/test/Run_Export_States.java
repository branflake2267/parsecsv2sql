package com.tribling.csv2sql.test;

import java.io.File;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;
import com.tribling.csv2sql.data.Export;

public class Run_Export_States {

  public static void main(String[] args) {
    
    // source 
    DatabaseData database_src = new DatabaseData(DatabaseData.TYPE_MYSQL, "192.168.10.79", "3306", "test", "test*7", "system");
    String table = "states";
    String whereSql = null;
    String limitSql = null;
    
    // destination
    File desDir = new File("/home/branflake2267/workspace/Csv2Sql/data/export");
    
    // prune these columns
    //ColumnData[] pruneColumnData = new ColumnData[1];
    //pruneColumnData[0] = new ColumnData();
    
    Export export = new Export(database_src, desDir);
    
    //export.setPruneColumns(pruneColumnData);
    //export.setShowCreateTable(false);
    //export.setSkipPrimaryKey(true); // export the primaryKey

    export.setTable(table, whereSql, limitSql);
    export.run(Export.EXPORTAS_CSV);
    
    export.setShowCreateTable(true);
    export.run(Export.EXPORTAS_SQL);
  }
  
}
