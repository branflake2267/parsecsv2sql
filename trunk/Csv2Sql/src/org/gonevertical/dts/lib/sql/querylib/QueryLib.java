package org.gonevertical.dts.lib.sql.querylib;

import java.math.BigDecimal;
import java.sql.ResultSet;

import org.gonevertical.dts.data.DatabaseData;

public interface QueryLib {

  public String getType();
  
  public String escape(String s);
  
  public String escape(int i);
  
  public int getResultSetSize(ResultSet result);
  
  public boolean queryBoolean(DatabaseData dd, String sql);
  
  public int queryInteger(DatabaseData dd, String sql);
  
  public long queryLong(DatabaseData dd, String sql);
  
  public String queryString(DatabaseData dd, String sql);
  
  public double queryDouble(DatabaseData dd, String sql);
  
  public BigDecimal queryBigDecimal(DatabaseData dd, String sql);
  
  public String queryIntegersToCsv(DatabaseData dd, String sql, char delimiter);

  public String queryStringToCsv(DatabaseData dd, String sql, char delimiter);
  
  public long update(DatabaseData dd, String sql);
  
  public long update(DatabaseData dd, String sql, boolean getKey);
  
  public boolean queryStringAndConvertToBoolean(DatabaseData dd, String sql);
  
  public boolean queryLongAndConvertToBoolean(DatabaseData dd, String sql);
  
}
