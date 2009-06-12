package com.tribling.csv2sql.lib.text;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {

  private String text = null;

  private int wordCount = 0;

  private String[] split = null;

  private ArrayList<String> combos = new ArrayList<String>();

  public TextParser() {

  }

  public void setText(String text) {

    if (text == null) {
      System.out.println("can't work with null");
      return;
    }

    this.text = text;
    this.text = this.text.trim();
    clean();
    countWords();
  }

  public void runThree() {

    if (text == null) {
      System.out.println("can't work with null");
      return;
    }

    // 0,3
    // 1,4
    // 2,5...
    int start = 0;
    int end = 3;
    for (int i = 0; i < wordCount; i++) {

      String s = getTextWords(start, end);
      System.out.println("words:" + s);

      start++;
      end++;

      if (end > wordCount) {
        break;
      }

    }
  }

  private String getTextWords(int start, int end) {

    // 0,0 = first word
    /*
     * String re = "^(?:\\S+\\s+){"+start+"}(\\S+)\\040+(\\S+\\s+){"+end+"}";
     * 
     * Pattern p = Pattern.compile(re); Matcher m = p.matcher(text); boolean
     * found = m.find();
     * 
     * String s = null; if (found == true) { s = m.group(1) + " "; s +=
     * m.group(2); }
     */

    String s = "";
    for (int i = start; i < end; i++) {
      s += split[i] + " ";
    }

    s = s.trim();

    combos.add(s);

    return s;
  }

  private void clean() {
    text = text.replaceAll("\\.", " ");
    text = text.replaceAll("\\!", " ");
    text = text.replaceAll("\\?", " ");
    text = text.replaceAll("[0-9%]", " ");
    text = text.replaceAll("\t", " ");
    text = text.replaceAll("\n", " ");
    text = text.replaceAll("\r", " ");
    text = text.replaceAll("'", "");
    text = text.replaceAll("[\\W]", " ");
    
    //System.out.println("CLEAN: " + text);
  }
  
  public String getText() {
    return this.text;
  }

  private void countWords() {
    split = text.trim().split("[\040]+");
    wordCount = split.length;
  }

  public ArrayList<String> getCombos() {
    return combos;
  }

}
