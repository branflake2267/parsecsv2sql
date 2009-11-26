package org.gonevertical.csv2sql.lib.experimental;

import java.sql.ResultSet;

import org.gonevertical.csv2sql.lib.StringUtil;


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

  private String[] regex = null;
  
  // more sql data
  private ResultSet rs = null;

  public Record() {
  }
  
  public void setData(ResultSet rs, String[] gbvalues, double[] cfvalues) {
    this.rs  = rs;
    this.gbvalues = gbvalues;
    this.cfvalues = cfvalues;
  }
  
  public void setRegex(String[] regex) {
    this.regex = regex;
  }
  
  public String[] getGroubByValues() {
    return this.gbvalues;
  }
  
  public double[] getCountValues() {
    return this.cfvalues;
  }

  public void sumCounts(double[] addcfvalues) {
    
    // TODO - testing regex found
    for (int i=0; i < cfvalues.length; i++) {
      System.out.println("cfval: " + cfvalues[i]);
      if (regex != null && regex[i] != null && test(regex[i], cfvalues[i])) {
        cfvalues[i] = cfvalues[i] + addcfvalues[i]; 
      } else {
        //cfvalues[i] = cfvalues[i] + addcfvalues[i];
      }
      
    }
    
  }

  private boolean test(String regex, double d) {
    
    boolean found = StringUtil.findMatch(regex, Double.toString(d));
    
    return found;
  }
  
  


  
}
