package org.gonevertical.csv2sql.lib.sql.querymulti;

import org.gonevertical.csv2sql.lib.sql.querylib.QueryLib;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class QueryMulti {

  private QueryLib queryLibMySql;
  private QueryLib queryLibMsSql;

  @Inject
  public void setMySqlEngine(@Named("MySql") QueryLib queryLib) {
      this.queryLibMySql = queryLib;
  }

  @Inject
  public void setMsSqlEngine(@Named("MsSql") QueryLib queryLib) {
      this.queryLibMsSql = queryLib;
  }

  public QueryLib getQueryLib_MySql() {
    return queryLibMySql;
  }
  
  public QueryLib getQueryLib_MsSql() {
      return queryLibMsSql;
  }
}
