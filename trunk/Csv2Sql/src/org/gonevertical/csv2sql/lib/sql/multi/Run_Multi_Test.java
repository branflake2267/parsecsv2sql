package org.gonevertical.csv2sql.lib.sql.multi;


import com.google.inject.Guice;
import com.google.inject.Injector;

public class Run_Multi_Test {

  public static void main(String[] args) {
    
    Injector injector = Guice.createInjector(new QueryModule());
    
    QueryMultiUtil queryLib = injector.getInstance(QueryMultiUtil.class);
    
    String type = queryLib.getQueryLib_MySql().getType();
    System.out.println("TYPE: " + type);
    
    String type2 = queryLib.getQueryLib_MsSql().getType();
    System.out.println("TYPE2: " + type2);
  }
  
  
}
