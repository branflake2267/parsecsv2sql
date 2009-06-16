package com.tribling.csv2sql.lib.text;

import java.util.ArrayList;

public class TextCleaner {
 
  private ArrayList<String> removeRegex = null;
  
  private String text = null;

  public TextCleaner() {
  }
  
  public void setText(String text) {
    this.text  = text;
  }
  
  /**
   * set patterns to remove
   * 
   * @param regex
   */
  public void setRemoveRegex(String regex) {
    if (removeRegex == null) {
      removeRegex = new ArrayList<String>();
    }
    removeRegex.add(regex);
  }
  
  public void clean() {
    
    removeLineBreaks();
    removeRegex();
    fixWordSpacing();
    
  }
  
  public String getText() {
    return this.text;
  }
  
  private void removeLineBreaks() {
    text = text.replaceAll("\n", " ");
    text = text.replaceAll("\r", " ");
    text = text.replaceAll("\t", " ");
  }
  
  private void removeRegex() {
    
    if (removeRegex == null) {
      return;
    }
    
    for (int i=0; i < removeRegex.size(); i++) {
      removeRegex(removeRegex.get(i));
    }
    
  }
  
  private void removeRegex(String regex) {
    text = text.replaceAll(regex, " ");
  }
  
  private void fixWordSpacing() {
    text = text.replaceAll("[\040]+", " ");
  }
}
