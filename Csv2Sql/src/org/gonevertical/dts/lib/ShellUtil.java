package org.gonevertical.dts.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class ShellUtil {
	
	private Logger logger = Logger.getLogger(ShellUtil.class);
  
  public void exec(String command) {
    try {
      Runtime.getRuntime().exec(command);
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
  
  public void exec_output(String command) {
    try {
      Process p = Runtime.getRuntime().exec(command);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      String s = "";
      while ((s = stdInput.readLine()) != null) {
        System.out.println(s);
      }
      while ((s = stdError.readLine()) != null) {
        System.out.println(s);
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}
