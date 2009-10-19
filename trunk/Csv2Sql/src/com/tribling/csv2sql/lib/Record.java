package com.tribling.csv2sql.lib;

import java.sql.ResultSet;

/**
 * record of values to aggregate
 * 
 * @author branflake2267
 *
 */
public class Record  {

  // group by columns
  private String[] gbvalues = null;
  
  // count these columns
  private double[] cfvalues = null;

  // more sql data
  private ResultSet rs = null;

  public Record() {
  }
  
  public void setData(ResultSet rs, String[] gbvalues, double[] cfvalues) {
    this.rs  = rs;
    this.gbvalues = gbvalues;
    this.cfvalues = cfvalues;
  }
  
  public String[] getGroubByValues() {
    return this.gbvalues;
  }
  
  public double[] getCountValues() {
    return this.cfvalues;
  }

  public void sumCounts(double[] addcfvalues) {
    
    for (int i=0; i < cfvalues.length; i++) {
      cfvalues[i] = cfvalues[i] + addcfvalues[i];
    }
    
  }
  


  
}
