package com.tribling.csv2sql.lib.sql;

import com.tribling.csv2sql.data.ColumnData;
import com.tribling.csv2sql.data.DatabaseData;

/**
 * alter columns has several steps easier done in its own class
 * 
 * @author branflake2267
 *
 */
public class MySqlTransformAlterUtil extends MySqlTransformUtil {

  public MySqlTransformAlterUtil() {
  }
  
  // TODO move this to its own class
  public void alterColumn(DatabaseData dd, String table, ColumnData column, String columnType) {

    // 1. does column have index on it
    // 2. remember index
    // 3. alter column
    // 4. restore index
    
  }
  
  // TODO move this to its own class
  public void alterColumns(DatabaseData dd, String table, ColumnData[] columns, String columnType) {

    // 1. do columns have indexs
    // 2. remember indexs
    // 3. alter column
    // 4. restore index
    
  }
  
}
