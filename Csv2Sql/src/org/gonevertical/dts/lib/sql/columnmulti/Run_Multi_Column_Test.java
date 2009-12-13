package org.gonevertical.dts.lib.sql.columnmulti;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Run_Multi_Column_Test {

  public static void main(String[] args) {
    
    Injector injector = Guice.createInjector(new ColumnModule());
    ColumnMulti columnMulti = injector.getInstance(ColumnMulti.class);
    
    String type = columnMulti.getColumnLib_MySql().getType();
    System.out.println("TYPE: " + type);
    
    String type2 = columnMulti.getColumnLib_MsSql().getType();
    System.out.println("TYPE2: " + type2);
  }
  
}
