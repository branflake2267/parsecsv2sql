package org.gonevertical.dts.lib;

import java.io.IOException;

public class ShellUtil {

  public void exec(String command) {
    try {
      Process child = Runtime.getRuntime().exec(command);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}