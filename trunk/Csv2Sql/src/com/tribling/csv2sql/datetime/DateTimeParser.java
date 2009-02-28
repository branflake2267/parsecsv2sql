package com.tribling.csv2sql.datetime;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO not sure how I will do the international months yet
 * 
 * Experimental stage
 */
public class DateTimeParser {

  /*
   * some formats I have seen. php strtotime is the ultimate
   * 12/01/2009
   * 2009/12/01
   * 01/12/2009
   * jan 01 2009
   * January 01, 2009
   * 2009-12-01 00:00:00
   * 2009-12-01 00:00:00AM
   * 20091201000000
   */

  // date string given
  private String datetime = null;

  // date object created from date string given
  private Date date = null;

  // return type mm/dd/yyyy
  public final static int TYPE_ENG_DATE = 1;
  
  // return type yyyy-MM-dd HH:MM:SS
  public final static int TYPE_MYSQL_DATETIME = 2;

  /**
   * constructor
   */
  public DateTimeParser() {
  }

  public Date getDate(String dt) {
    // type doesn't matter, b/c returning date
    getDate(TYPE_ENG_DATE);
    return date;
  }

  /**
   * get datetime in string mm/dd/yyyy
   * @return
   */
  public String getDate_EngString(String datetime) {
    this.datetime = datetime;
    String s = getDate(TYPE_ENG_DATE);
    return s;
  }

  public String getDateMysql(String datetime) {
    this.datetime = datetime;
    String s = getDate(TYPE_MYSQL_DATETIME);
    return s;
  }
  
  private String getDate(int type) {
    
    if (datetime == null) {
      return "";
    }
    
    datetime = datetime.trim();
    
    // reset it just in case
    date = null;
    
    DateFormat df = null;
    if (type == TYPE_ENG_DATE) {
      df = new SimpleDateFormat("MM/dd/yyyy");
      
    } else if (type == TYPE_MYSQL_DATETIME) {
      df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
    
    String s = "";
    if (checkforFormat_monthyear() == true) {  
      s = df.format(date);

    } else if (checkforFormat_common() == true ) {
      s = df.format(date);
      
    } else if (checkforFormat_common2() == true ) {
      s = df.format(date);
      
    } else if (checkforFormat_monthyear2() == true) { 
      s = df.format(date);
      
    } else if (checkforFormat_custom() == true) { 
      s = df.format(date);
      
    } else {
    
      // return orginal if not matched
      s = datetime;
    }
    
    return s;
  }

  /**
   * check format jan-09 or january-09
   * 
   * @return found
   */
  public boolean checkforFormat_monthyear() {

    //jan-09 or january-09 or jan 09 or jan 2009  jan09 or Jan2009  
    // TODO  jan 01 2009 or jan 01 09?? in another matching type
    String re = "([a-zA-Z]+)[\\-\040]?([0-9]+)";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(datetime);
    boolean found = m.find();

    int month = 0;
    int year = 0;
    if (found == true) {
      String mm = m.group(1);
      String yy = m.group(2);
      
      if (mm == null | yy == null) {
        return false;
      }
      
      month = getMonth(mm) - 1;
      year = getYear(yy);
    } else {
      return false;
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);

    date = cal.getTime();

    return found;
  }
  
  public boolean checkforFormat_monthyear2() {

    // jan 01 2009 or jan 01 09
    String re = "([a-zA-Z]+)[\\-\040/]([0-9]+)[\\-\040/]([0-9]+)";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(datetime);
    boolean found = m.find();

    int month = 0;
    int day = 0;
    int year = 0;
    if (found == true) {
      String mm = m.group(1);
      String dd = m.group(2);
      String yy = m.group(3);
      
      if (mm == null | dd == null | yy == null) {
        return false;
      }
      
      month = getMonth(mm) - 1;
      day = getDay(dd);
      year = getYear(yy);
      
    } else {
      return false;
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);

    date = cal.getTime();

    return found;
  }

  /**
   * check for common string format mm/dd/yy or mm/dd/yyyy
   * 
   * @return found
   */
  public boolean checkforFormat_common() {

    // mm/dd/yyyy
    String re = "([0-9]+)[/\\-\040\\.]([0-9]+)[/\\-\040\\.]([0-9]+)";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(datetime);
    boolean found = m.find();

    int month = 0;
    int day = 0;
    int year = 0;
    if (found == true) {
      String mm = m.group(1);
      String dd = m.group(2);
      String yy = m.group(3);
      
      if (mm == null | dd == null | yy == null) {
        return false;
      }
      
      month = getMonth(mm) - 1;
      day = getDay(dd);
      year = getYear(yy);
      
    } else {
      return false;
    }
    
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);

    date = cal.getTime();

    return found;
  }

  public boolean checkforFormat_common2() {

    // mm/yyyy
    String re = "([0-9]+)[/\\-\040\\.]([0-9]+)";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(datetime);
    boolean found = m.find();

    int month = 0;
    int year = 0;
    if (found == true) {
      String mm = m.group(1);
      String yy = m.group(2);
      
      if (mm == null | yy == null) {
        return false;
      }
      
      month = getMonth(mm) - 1;
      year = getYear(yy);
    } else {
      return false;
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);

    date = cal.getTime();

    return found;
  }
  
  /**
   * custom format -bsgbB620090200054003 or 20090200054003
   * @return
   */
  public boolean checkforFormat_custom() {

    String re = ".*?([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$";
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(datetime);
    boolean found = m.find();

    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int min = 0;
    int sec = 0;
    if (found == true) {
      String yy = m.group(1);
      String mm = m.group(2);
      String dd = m.group(3);
      String hh = m.group(4);
      String mi = m.group(5);
      String ss = m.group(6);
      
      if (yy == null | mm == null | dd == null | hh == null | mi == null | ss == null) {
        return false;
      }
      
      year = getYear(yy);
      month = getMonth(mm) - 1;
      day = getDay(dd);
      hour = getTimeValue(hh);
      min = getTimeValue(mi);
      sec = getTimeValue(ss);
    } else {
      return false;
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR, hour);
    cal.set(Calendar.MINUTE, min);
    cal.set(Calendar.SECOND, sec);

    date = cal.getTime();

    return found;
  }
  
  private int getMonth(String s) {
    int month = -1;
    if (s.matches("[0-9]+")) {
      month = Integer.parseInt(s);
    } else if (s.contains("jan")) {
      month = 1;
    } else if (s.contains("feb")) {
      month = 2;
    } else if (s.contains("mar")) {
      month = 3;
    } else if (s.contains("apr")) {
      month = 4;
    } else if (s.contains("may")) {
      month = 5;
    } else if (s.contains("jun")) {
      month = 6;
    } else if (s.contains("jul")) {
      month = 7;
    } else if (s.contains("aug")) {
      month = 8;
    } else if (s.contains("sep")) {
      month = 9;
    } else if (s.contains("oct")) {
      month = 10;
    } else if (s.contains("nov")) {
      month = 11;
    } else if (s.contains("dec")) {
      month = 12;
    }
    return month;
  }

  private int getYear(String s) {

    int year = -1;
    if (s.matches("[0-9]{2}")) {
      year = 2000 + Integer.parseInt(s);
    } else if (s.matches("[0-9]{4}")) {
      year = Integer.parseInt(s);
    } 

    return year;
  }

  private int getDay(String s) {
    int day = Integer.parseInt(s);
    return day;
  }

  private int getTimeValue(String s) {
    int t = Integer.parseInt(s);
    return t;
  }




  /**=p
   * TODO - more to do
   * 
   * @param s
   * @return
   */
  private boolean isDate(String s) {

    s = s.toLowerCase();

    if (s.length() == 0) {
      return false;
    }

    boolean b = false;

    if (s.contains("jan")) {
      b = true;
    } else if (s.contains("feb")) {
      b = true;
    } else if (s.contains("feb")) {
      b = true;
    } else if (s.contains("mar")) {
      b = true;
    } else if (s.contains("apr")) {
      b = true;
    } else if (s.contains("may")) {
      b = true;
    } else if (s.contains("jun")) {
      b = true;
    } else if (s.contains("jul")) {
      b = true;
    } else if (s.contains("aug")) {
      b = true;
    } else if (s.contains("sep")) {
      b = true;
    } else if (s.contains("oct")) {
      b = true;
    } else if (s.contains("nov")) {
      b = true;
    } else if (s.contains("dec")) {
      b = true;
    }

    // TODO - proof this later
    if (s.matches("[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4}.*")) {
      b = true;
    }

    return b;
  }

  /**
   * another option to parse the date
   * @return
   */
  private boolean checkforFormat_montYear2() {
    DateFormat df = new SimpleDateFormat("MMM-YYYY");

    try {
      Date today = df.parse("jan-2009");            
      System.out.println("Today = " + df.format(today));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    return false;
  }

  private String test() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    return dateFormat.format(date);
  }


}
