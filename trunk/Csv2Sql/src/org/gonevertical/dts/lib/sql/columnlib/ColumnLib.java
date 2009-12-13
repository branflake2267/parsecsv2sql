package org.gonevertical.dts.lib.sql.columnlib;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.gonevertical.dts.data.ColumnData;
import org.gonevertical.dts.data.ColumnDataComparator;
import org.gonevertical.dts.data.DatabaseData;
import org.gonevertical.dts.lib.sql.MySqlQueryUtil;
import org.gonevertical.dts.lib.sql.MySqlTransformUtil;

public interface ColumnLib {

  public String getType();
  
  public ColumnData[] getResult(ResultSet result, ColumnData[] columnData);

  public ColumnData[] getResult(ResultSet result, ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getSql_Names(ColumnData[] columnData);

  public String getSql_Names(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getSql_Names_WSql(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getCsv_Names(ColumnData[] columnData);
  
  public String getCsv_Names(ColumnData[] columnData, ColumnData[] pruneColumnData);
  
  public String getCsv_Values(ColumnData[] columnData);
  
  public String getCsv_Values(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getSql(ColumnData[] columnData);

  public String getSql(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getSql_Insert(ColumnData[] columnData);
  
  public String getSql_Insert(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public String getSql_Update(ColumnData[] columnData);

  public String getSql_Update(ColumnData[] columnData, ColumnData[] pruneColumnData);
 
  public String getSql_GetMaxCharLength(DatabaseData dd, ColumnData columnData);

  public ColumnData[] prune(ColumnData[] columnData, ColumnData[] pruneColumnData);

  public ColumnData[] prunePrimaryKey(ColumnData[] columnData);
 
  public boolean doesColumnExist(ColumnData[] searchColumnData, ColumnData forColumnData);

  public int searchColumnByName_NonComp(ColumnData[] searchColumnData, ColumnData forColumnData);
  
  public int searchColumnByName_UsingComparator(ColumnData[] searchColumnData, ColumnData forColumnData);

  public int searchColumnByName_UsingComparator(ColumnData[] searchColumnData, String columnName);

  public boolean doesColumnNameExist(ColumnData[] searchColumnData, String forColumnName);
  
  public String getPrimaryKey_Value(ColumnData[] columnData);

  public String getPrimaryKey_Name(ColumnData[] columnData);
  
  public int getPrimaryKey_Index(ColumnData[] columnData);
  
  public ColumnData getPrimaryKey_ColumnData(ColumnData[] columnData);
  
  public ColumnData[] addValues(ColumnData[] columnData, String[] values);
  
  public String getSql_IdentitiesWhere(ColumnData[] columnData);
 
  public String getSql_IdentitiesIndex(DatabaseData dd, ColumnData[] columnData);
  
  public String getSql_Index_Multi(ColumnData[] columns);
  
  public ColumnData[] getColumns_Identities(ColumnData[] columnData);
  
  public ColumnData[] merge(ColumnData[] columnData, ColumnData[] addColumnData);

  public ColumnData[] merge(ColumnData[] columnData, ColumnData addColumnData);
  
  public String getSql_AlterColumns(DatabaseData dd, ColumnData[] columnData);
  
  public String getSql_ModifyColumns(ColumnData[] columnData);
  
  public ColumnData[] getColumns_Distinct(ColumnData[] columnData);
  
}
