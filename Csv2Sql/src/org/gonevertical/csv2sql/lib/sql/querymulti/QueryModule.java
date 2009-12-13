package org.gonevertical.csv2sql.lib.sql.querymulti;

import org.gonevertical.csv2sql.lib.sql.querylib.MsSqlQueryLib;
import org.gonevertical.csv2sql.lib.sql.querylib.MySqlQueryLib;
import org.gonevertical.csv2sql.lib.sql.querylib.QueryLib;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;


  public class QueryModule implements Module {

    @Override
    public void configure(Binder binder) {
      
      binder.bind(QueryLib.class).annotatedWith(Names.named("MySql")).to(MySqlQueryLib.class);
      binder.bind(QueryLib.class).annotatedWith(Names.named("MsSql")).to(MsSqlQueryLib.class);
      
    }
    
  }
  

