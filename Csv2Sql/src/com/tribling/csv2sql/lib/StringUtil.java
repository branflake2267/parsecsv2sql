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
  
  public static String getValue(String regex, String value) {
    if (regex == null | value == null) {
      return null;
    }
    
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(value);
      boolean found = m.find();
      if (found == true) {
        value = m.group(1);
      }
    } catch (Exception e) {
      System.out.println("findMatch: regex error");
    }
    
    return value;
  }
  
  /**
   * string array to csv using now quotes
   * @param s
   * @return
   */
  public static String toCsv_NoQuotes(String[] s) {
    if (s == null) {
      return null;
    }
    int l = s.length;
    String r = null;
    if (l == 1) {
      r = s[0];
    } else {
      
      for (int i=0; i < s.length; i++) {
        r += s[0];
        if (i < s.length-1) {
          r += ",";
        }
      }
      
    }
    
    return r;
  }
  
}
