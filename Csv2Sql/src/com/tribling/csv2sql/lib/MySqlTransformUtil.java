package com.tribling.csv2sql.lib;

import com.tribling.csv2sql.data.DatabaseData;

public class MySqlTransformUtil extends MySqlQueryUtil {

  public MySqlTransformUtil() {
  }

  /**
   * does table exist?
   * 
   * @param dd
   * @param table
   * @return
   */
  public static boolean doesTableExist(DatabaseData dd, String table) {
    String query = "SHOW TABLES FROM `" + dd.getDatabase() + "` LIKE '" + table + "';";
    return queryStringAndConvertToBoolean(dd, query);
  }
  
  /**
   * fix table field name
   * @param tableorcolumn
   * @return
   */
  public static String fixName(String tableorcolumn) {

    if (tableorcolumn.length() > 64) {
      tableorcolumn = tableorcolumn.substring(0, 63);
    }
    tableorcolumn = tableorcolumn.trim();

    tableorcolumn = tableorcolumn.replace("'", "");
    tableorcolumn = tableorcolumn.replace("[\"\r\n\t]", "");
    tableorcolumn = tableorcolumn.replace("!", "");
    tableorcolumn = tableorcolumn.replace("@", "");
    tableorcolumn = tableorcolumn.replace("#", "_Num");
    tableorcolumn = tableorcolumn.replace("$", "");
    tableorcolumn = tableorcolumn.replace("^", "");
    tableorcolumn = tableorcolumn.replace("\\*", "");
    tableorcolumn = tableorcolumn.replace("\\", "");
    tableorcolumn = tableorcolumn.replace("\\+", "");
    tableorcolumn = tableorcolumn.replace("=", "");
    tableorcolumn = tableorcolumn.replace("~", "");
    tableorcolumn = tableorcolumn.replace("`", "");
    tableorcolumn = tableorcolumn.replace("\\{", "");
    tableorcolumn = tableorcolumn.replace("\\}", "");
    tableorcolumn = tableorcolumn.replace("\\[", "");
    tableorcolumn = tableorcolumn.replace("\\]", "");
    tableorcolumn = tableorcolumn.replace("\\|", "");
    tableorcolumn = tableorcolumn.replace(".", "_");
    tableorcolumn = tableorcolumn.replace(",", "");
    tableorcolumn = tableorcolumn.replace("\\.", "");
    tableorcolumn = tableorcolumn.replace("<", "");
    tableorcolumn = tableorcolumn.replace(">", "");
    tableorcolumn = tableorcolumn.replace("?", "");
    tableorcolumn = tableorcolumn.replace("&", "");
    tableorcolumn = tableorcolumn.replace("/", "");
    tableorcolumn = tableorcolumn.replace("%", "_per");
    tableorcolumn = tableorcolumn.replace(" ", "_");
    tableorcolumn = tableorcolumn.replace("(", "");
    tableorcolumn = tableorcolumn.replace(")", "");
    tableorcolumn = tableorcolumn.replaceAll("(\\W)", "");
    //s = s.replaceAll("(\\-)", "");

    return tableorcolumn;
  }

}
