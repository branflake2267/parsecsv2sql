package com.tribling.csv2sql.data;

import java.util.Comparator;

public class ColumnDataComparator implements Comparator<ColumnData> {

  public static final int NAME = 1;
  
  // default type is by column name
  public int by = NAME;
  
  public void setBy(int by) {
    this.by = by;
    if (by == 0) {
      by = NAME;
    }
  }
  
  public int compare(ColumnData a, ColumnData b) {
    
    int c = 0;
    
    if (by == NAME) {
      String ac = a.getColumnName();
      String bc = a.getColumnName();
      c = ac.compareTo(bc);
    }

    return c;
  }

}
