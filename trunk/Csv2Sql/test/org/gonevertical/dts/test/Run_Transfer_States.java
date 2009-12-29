package org.gonevertical.dts.test;

import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.process.Transfer;

public class Run_Transfer_States {
  
  public static void main(String[] args) {
    
    new Run_Transfer_States().run();
    
  }

  private void run() {
    
    DatabaseData dd_src = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    DatabaseData dd_dst = new DatabaseData(DatabaseData.TYPE_MYSQL, "ark", "3306", "test", "test#", "test");
    
    String fromTable = "import_zipcodes_test";
    String toTable = "import_zipcodes_test_transfer";
    
    Transfer transfer = new Transfer(dd_src, dd_dst);
    transfer.transferAllFields(fromTable, toTable);
    
  }
  
}
