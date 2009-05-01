package com.tribling.csv2sql.lib;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

  
  /**
   * find a match in a string
   * 
   * @param regex
   * @param s
   * @return
   */
  public static boolean findMatch(String regex, String s) {
    if (regex == null | s == null) {
      return false;
    }
    boolean found = false;
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(s);
      found = m.find();
    } catch (Exception e) {
      System.out.println("findMatch: regex error");
      found = false;
    }
    return found;
  }
  
}
