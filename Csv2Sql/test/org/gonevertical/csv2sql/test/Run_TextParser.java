package org.gonevertical.csv2sql.test;

import org.gonevertical.csv2sql.lib.text.WordParser;

public class Run_TextParser {
  
  public static void main(String[] args) {
    
    WordParser process = new WordParser();
    process.setText("I went to the store. Hi I am brandon.");
    int comboSize = 3;
    process.run(comboSize);
    
  }

}
