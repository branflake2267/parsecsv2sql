package org.gonevertical.dts.lib;

import java.util.Calendar;

import sun.util.calendar.LocalGregorianCalendar.Date;

public class SqlUtil {

  /**
   * get calendar as string like '2010-08-14 14:21:05'
   * 
   * @param cal
   * @return
   */
  public static String getCalendarAsSqlDt(Calendar cal) {
    if (cal == null) {
      return null;
    }
    
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH) + 1;
    int day = cal.get(Calendar.DATE);
    int hh = cal.get(Calendar.HOUR);
    int min = cal.get(Calendar.MINUTE);
    int sec = cal.get(Calendar.SECOND);
    
    String s = year + "-" + month + "-" + day + " " + hh + ":" + min + ":" + sec;
    
    return s;
  }
  
  /**
   * get date as string like '2010-08-14 14:21:05'
   * 
   * @param date
   * @return
   */
  public static String getDateAsSqlDt(java.util.Date date) {
    if (date == null) {
      return null;
    }
    
    int year = date.getYear() + 1900;
    int month = date.getMonth() + 1;
    int day = date.getDate();
    int hh = date.getHours();
    int min = date.getMinutes();
    int sec = date.getSeconds();
    
    String s = year + "-" + month + "-" + day + " " + hh + ":" + min + ":" + sec;
    
    return s;
  }
  
}
