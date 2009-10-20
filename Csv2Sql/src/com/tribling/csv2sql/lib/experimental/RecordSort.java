package com.tribling.csv2sql.lib.experimental;

import java.util.Comparator;

public class RecordSort implements Comparator<Record> {

  public int compare(Record a, Record b) {
    
    int c = 0;
    
    String[] aGb = a.getGroubByValues();
    String[] bGb = b.getGroubByValues();
    for (int i=0; i < aGb.length; i++) {
      c += aGb[i].compareTo(bGb[i]);
    }
    
    return c;
  }

}
