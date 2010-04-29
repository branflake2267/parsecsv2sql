package org.gonevertical.dts.lib;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class StringUtil {
	
	private Logger logger = Logger.getLogger(StringUtil.class);

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
    
    // TODO - tell teh person when he doesn't put in (group) parentheses to capture group
    
    String v = null;
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(value);
      boolean found = m.find();
      if (found == true) {
        v = m.group(1);
      }
    } catch (Exception e) {
      System.out.println("findMatch: regex error (check to see if you have a (group)");
      e.printStackTrace();
    }
    
    return v;
  }
  
  public static String[] getValues(String regex, String value) {
    if (regex == null | value == null) {
      return null;
    }
    
    ArrayList<String> s = new ArrayList<String>();
    try {
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(value);
      
      	while (m.find()) {
	        s.add(m.group(1));
        }
      	
    } catch (Exception e) {
      System.out.println("findMatch: regex error (check to see if you have a (group)");
      e.printStackTrace();
    }
    
    if (s.size() == 0) {
    	return null;
    }
    
    String[] r = new String[s.size()];
    s.toArray(r);
    
    return r;
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
    String r = "";
    if (l == 1) { // only one
      r = s[0];
    } else {
      for (int i=0; i < s.length; i++) {
        r += s[i];
        if (i < s.length-1) {
          r += ",";
        }
      }
    }
    if (r != null && r.trim().length() == 0) {
      return null;
    }
    return r;
  }
  
}
