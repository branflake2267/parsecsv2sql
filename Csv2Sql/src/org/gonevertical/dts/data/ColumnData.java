package org.gonevertical.dts.data;

import java.math.BigDecimal;

import org.apache.commons.lang.WordUtils;
import org.gonevertical.dts.lib.StringUtil;
import org.gonevertical.dts.lib.datetime.DateTimeParser;
import org.gonevertical.dts.lib.sql.transformlib.MySqlTransformLib;


public class ColumnData {

  // change value case
  public final static int CHANGECASE_LOWER = 1;
  public final static int CHANGECASE_UPPER = 2;
  public final static int CHANGECASE_SENTENCE = 3;

  // column data types 
  public static final int FIELDTYPE_DATETIME = 1;
  public static final int FIELDTYPE_VARCHAR = 2;
  public static final int FIELDTYPE_INT_ZEROFILL = 3;
  public static final int FIELDTYPE_INT = 4;
  public static final int FIELDTYPE_DECIMAL = 5;
  public static final int FIELDTYPE_EMPTY = 6;
  public static final int FIELDTYPE_TEXT = 7;


  // type of index
  public static final int INDEXKIND_DEFAULT = 1;
  public static final int INDEXKIND_FULLTEXT = 2;

  // is this column a primary key?
  private boolean isPrimaryKey = false;

  // when using multiple columns for identity, for similarity matching
  private boolean usedForIdentity = false;

  // column name
  // TODO public access to this var is deprecated, changing to method access
  public String column = "";

  private String columnAsSql = null;

  // column field type - like INTEGER DEFAULT 0
  // TODO public access to this var is deprecated, changing to method access
  public String columnType = "TEXT DEFAULT NULL";
  // new var to show what type of column it is
  public int fieldType = FIELDTYPE_TEXT;

  // column field length for the given column type
  // when type is set, this is discovered
  private int lengthChar = 0;

  // for decimal(left,right)
  private int lengthChar_left = 0;
  private int lengthChar_right = 0;

  // columns associated value
  private String value = null;

  // set the value as a function
  // this will replace value when looking for retreval
  private String valueIsFunction = null;

  // true:overwrite any value false:only update on blank
  private boolean overwriteOnlyWhenBlank = true;

  // true: if a zero shows up its ok to overwrite
  private boolean overwriteOnlyWhenZero = true;

  // set the value using regex
  private String regex = null;

  // table that the column resides in, optional
  private String table = null;

  // set with static constant above
  private int changeCase = 0;

  /**
   * constructor
   */
  public ColumnData() {
  }

  /**
   * constructor - create a column object 
   * 
   * @param columnTable table name
   * @param columnName column name
   * @param columnType columnType ie. [TEXT DEFAULT NULL]
   */
  public ColumnData(String columnTable, String columnName, String columnType) {
    setTable(columnTable);
    setColumnName(columnName);
    setType(columnType);
  }

  /**
   * set value of column
   * 
   * @param value
   */
  public void setValue(String value) {

    if (value != null && changeCase > 0) {
      value = changeCase(value);
    }

    // use regex to get the value
    if (regex != null) {
      value = StringUtil.getValue(regex, value);
    }
    this.value = value;

    if (value != null) {
      value.trim();
      if (value.length() == 0) {
        value = null;
      }
    }

  }

  /**
   * set value of column
   * 
   * @param value
   */
  public void setValue(Long value) {
    this.value = Long.toString(value);
  }

  private String changeCase(String value) {
    if (changeCase == CHANGECASE_LOWER) {
      value = value.toLowerCase();
    } else if (changeCase == CHANGECASE_UPPER) {
      value = value.toUpperCase();
    } else if (changeCase == CHANGECASE_SENTENCE) {
      value = WordUtils.capitalizeFully(value);
    }
    return value;
  }

  /**
   * get value
   *   will return valueIsFunction if set, this will overide value
   * 
   * @return
   */
  public String getValue() {
    String v = null;

    if (valueIsFunction != null) {
      v = valueIsFunction;
    } else {
      v = this.value;
    }

    // the next only affect given types
    // if type is int, check to be sure its an int
    if (columnType.toLowerCase().contains("int") == true | columnType.toLowerCase().contains("dec") == true) {
      v = getValueAsInt(value);
    }

    // if type is datetime, lets deal with it now
    if (columnType.toLowerCase().contains("datetime") == true) {
      v = getValueAsDateTime(value);
    }


    return v;
  }

  /**
   * when column type is datetime, transform it to datetime and be sure datetime correct on the way out
   *    TODO on setting datetime, do I parse it too? 
   * 
   * @param value
   * @return
   */
  private String getValueAsDateTime(String value) {
    if (columnType.toLowerCase().contains("datetime") == true) {
      if (value == null) {
        value = null;
      } else if (value.trim().length() == 0) {
        value = null;
      } else {
        // TODO move dtp to class instance var
        DateTimeParser dtp = new DateTimeParser();
        value = dtp.getDateMysql(value);
        if (dtp.isDate == false) {
          value = null;
        }
      }
    }
    return value;
  }

  /**
   * when column type is int - be sure to check the value is really an int
   * 
   * @param value
   * @return
   */
  public String getValueAsInt(String value) {

    if (columnType.toLowerCase().contains("int") == true) {
      if (value == null) {
        value = null;
      } else if (value.trim().length() == 0) {
        value = null;
      } else {
        try {
          // change (1234) to negative
          if (value != null && value.matches("[\\(].*[\\)]")) {
            value = value.replaceAll("[\\)\\(]", "");
            value = "-" + value;
          }
          // take out all non digit characters except . - and 0-9
          if (value != null) {
            value = value.replaceAll("[^0-9\\.\\-]", ""); 
          }
          BigDecimal bd = new BigDecimal(value);
          value = bd.toString();
        } catch (NumberFormatException e) {
          value = "0";
        }
      }
    } else if (columnType.toLowerCase().contains("dec") == true) {
      if (value == null) {
        value = null;
      } else if (value.trim().length() == 0) {
        value = null;
      } else {
        try { 
          // change (1234) to negative
          if (value != null && value.matches("[\\(].*[\\)]")) {
            value = value.replaceAll("[\\)\\(]", "");
            value = "-" + value;
          }
          // take out all non digit characters except . - and 0-9
          if (value != null) {
            value = value.replaceAll("[^0-9\\.\\-]", "");
          }
          BigDecimal bd = new BigDecimal(value);
          value = bd.toString();
        } catch (NumberFormatException e) {
          value = "0";
        }
      }
    }

    return value;
  }

  public int getValueLength() {
    int l = 0;
    if (value != null) {
      l = value.length();
    }
    return l;
  }

  public String getColumnName() {
    return column;
  }

  public void setColumnAsSql() {
    if (column.toLowerCase().matches(".*[\040]as[\040].*") == false) {
      return;
    }

    // save (select *...) as sql
    columnAsSql = column;

    String regex = ".*[\040]as[\040](.*)";
    String c = StringUtil.getValue(regex, column.toLowerCase());
    if (c != null) {  
      column = c.trim();
    }

  }

  public String getColumnAsSql() {
    return columnAsSql;
  }


  public void setColumnName(String columnName) {
    this.column = columnName;
    setColumnAsSql(); // in case it has sql AS in it
  }

  public void setIsPrimaryKey(boolean b) {
    isPrimaryKey = b;
  }

  public boolean getIsPrimaryKey() {
    return isPrimaryKey;
  }

  public void setOverwriteWhenBlank(boolean b) {
    this.overwriteOnlyWhenBlank = b;
  }

  public boolean getOverwriteWhenBlank() {
    return this.overwriteOnlyWhenBlank;
  }

  public void setOverwriteWhenZero(boolean b) {
    this.overwriteOnlyWhenZero = b;
  }

  public boolean getOverwriteWhenZero() {
    return this.overwriteOnlyWhenZero;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

  public String getRegex() {
    return regex;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getTable() {
    return this.table;
  }

  /**
   * set with constant
   * 
   * @param changeCase
   */
  public void setCase(int changeCase) {
    this.changeCase = changeCase;
  }

  /**
   * set the value as function
   * 
   * @param sqlfunction
   */
  public void setValueAsFunction(String sqlfunction) {
    this.valueIsFunction = sqlfunction;
  }

  /**
   * get the value 
   */
  public String getValueAsFunction() {
    return valueIsFunction;
  }

  /**
   * is the value set as a function
   * @return
   */
  public boolean isFunctionSetForValue() {
    boolean b = false;
    if (valueIsFunction != null) {
      b = true;
    }
    return b;
  }

  /**
   * set this column as use as an identity, like three columns get set for similarity matching
   * 
   * @param b
   */
  public void setIdentity(boolean b) {
    this.usedForIdentity = b;
  }

  /**
   * is this column used for identity
   * 
   * @return
   */
  public boolean getIdentityUse() {
    return usedForIdentity;
  }

  /**
   * set column type and extract length
   * 
   * @param columnType
   */
  public void setType(String columnType) {
    this.columnType = columnType.toLowerCase();
    setCharLength(columnType);
  }

  /**
   * set the lengths INT(10) or decimal(10,2)
   * @param columnType
   */
  public void setCharLength(String columnType) {

    if (columnType.contains(",")) { // decimal column type
      lengthChar_left = 0;
      lengthChar_right = 0;
      setDecimalLengths(columnType);
    } else {
      String len = StringUtil.getValue("([0-9]+)", columnType);
      if (len != null && len.length() > 0) {
        lengthChar = Integer.parseInt(len);
      } else {
        lengthChar = 0;
      }
    }

  }

  private void setDecimalLengths(String s) {
    int l = 0;
    int r = 0;
    if (s.contains(",")) {
      String sv = StringUtil.getValue("([0-9]+,[0-9]+)", s);
      String[] a = sv.split(",");
      l = Integer.parseInt(a[0]);
      r = Integer.parseInt(a[1]);
    } else {
      l = s.length();
    }
    lengthChar = 0;
    lengthChar_left = l;
    lengthChar_right = r;
  }

  /**
   * get type with no changes
   * @return
   */
  public String getType() {
    return columnType;
  }

  /**
   * get the length of varchar(lengthChar)
   * 
   * @return
   */
  public int getCharLength() {
    return lengthChar;
  }

  /**
   * change the lengthChar
   *   used for altering smaller
   * @param resize
   */
  public void setCharLength(int resize) {
    this.lengthChar = resize;
  }

  /**
   * TODO - add all the column types
   * 
   * @return
   */
  public boolean doesValueFitIntoColumn() {
    columnType = columnType.toLowerCase();

    boolean b = false;
    if (columnType.contains("text") == true) {
      b = doesValueFit_Text();
    } else if (columnType.contains("varchar") == true) {
      b = doesValueFit_Varchar();
    } else if (columnType.contains("bigint") == true) {
      b = doesValueFit_BigInt();
    } else if (columnType.contains("tinyint") == true) {
      b = doesValueFit_TinyInt();
    } else if (columnType.contains("int") == true) {
      b = doesValueFit_Int();
    }  else if (columnType.contains("dec") == true) {
      b = doesValueFit_Decimal();
    } else if (columnType.contains("datetime") == true) {
      b = true; // date doesn't change
    } else {
      b = true;
    }
    // TODO - add more types

    return b;
  }

  private boolean doesValueFit_Text() {
    boolean b = false;
    if (value == null) {
      b = true;
    } else if (value.length() <= 65536) { //65536 bytes 2^16
      b = true;
    }
    return b;
  }

  private boolean doesValueFit_Varchar() {
    boolean b = false;
    if (value == null) {
      b = true;
    } else if (value.length() <= lengthChar) { // 255 bytes
      b = true;
    }
    return b;
  }

  private boolean doesValueFit_Int() {
    boolean b = false;
    if (value.length() <= 9) {
      b = true;
    }
    return b;
  }

  private boolean doesValueFit_TinyInt() {
    boolean b = false;
    if (value.length() <= 1) {
      b = true;
    }
    return b;
  }

  private boolean doesValueFit_BigInt() {
    boolean b = false;
    if (value.length() < 20) {
      b = true;
    }
    return b;
  }

  // TODO - this needs adjustment of the left value, and total values are different
  private boolean doesValueFit_Decimal() {
    //System.out.println("current left: " + lengthChar_left + " right: " + lengthChar_right);
    boolean b = true;
    char per = ".".charAt(0);
    if (value.contains(Character.toString(per))) {
      String[] a = value.split("\\.");
      int l = a[0].trim().length();
      int r = a[1].trim().length();

      //System.out.println("l: " + l + " r: " + r);
      if (r > lengthChar_right) {
        lengthChar_right = r;
        b = false;
      }

      l = l + lengthChar_right;

      if (l > lengthChar_left) {
        lengthChar_left = l;
        b = false;
      }
      //System.out.println("new left: " + lengthChar_left + " right: " + lengthChar_right);
    }
    return b;
  }

  /**
   * alter the column size if need be
   */
  public void alterColumnSizeBiggerIfNeedBe(DatabaseData dd) {
    if (value == null) {
      return;
    }
    // will the data fit
    boolean b = doesValueFitIntoColumn();
    if (b == true) {
      return;
    }
    // alter column size
    alterColumnToBiggerSize(dd);
  }

  private void alterColumnToBiggerSize(DatabaseData dd) {
    int l = value.getBytes().length;

    if (columnType.contains("text") == true) {
      if (l >= 255) {
        setType("TEXT DEFAULT NULL");
      } else if (columnType.contains("varchar") == true) {
        setType("VARCHAR(" + l + ") DEFAULT NULL");
      }
    } else if (columnType.contains("varchar") == true) {
      if (l >= 255) {
        setType("TEXT DEFAULT NULL");
      } else if (columnType.contains("varchar") == true) {
        setType("VARCHAR(" + l + ") DEFAULT NULL");
      }
    } else if (columnType.contains("int") == true) {
      if (value.length() < 8) {
        setType("INT DEFAULT 0");
      } else if (value.length() >= 8) {
        setType("BIGINT DEFAULT 0");
      } 
    } else if (columnType.contains("dec") == true) {
      setType("DECIMAL(" + lengthChar_left + "," + lengthChar_right + ") DEFAULT 0.0");

    } else if (columnType.contains("datetime") == true) {
      // never needed
    } else {
      System.out.println("skipping alterColumnToBiggerSize()");
      return;
    }

    new MySqlTransformLib().alterColumn(dd, this);
  }

  /**
   * fix the column name < 64 and characters that are SQL friendly
   */
  public void fixName() {
    if (column.length() > 64) {
      column = column.substring(0, 63);
    }
    column = column.trim();
    column = column.replaceAll("#", "_Num");
    column = column.replaceAll("%", "_per");
    column = column.replaceAll("\\.", "_");
    column = column.replaceAll(" ", "_");
    column = column.replaceAll("[^\\w]", "");
    column = column.replaceAll("[\r\n\t]", "");
    column = column.replaceAll("(\\W)", "");
  }

  /**
   * TODO - finish this
   * 
   * @param columnType
   * @return
   */
  public int getFieldType(String columnType) {

    String type = columnType.toLowerCase();

    int fieldType = 0;
    if (type.contains("text")) {
      fieldType = ColumnData.FIELDTYPE_VARCHAR;
    } else if (type.contains("date")) {
      fieldType = ColumnData.FIELDTYPE_DATETIME;
    } else if (type.contains("varchar")) {
      fieldType = ColumnData.FIELDTYPE_VARCHAR;
    } else if (type.contains("int")) {
      fieldType = ColumnData.FIELDTYPE_INT;
    } else if (type.contains("double") | type.contains("decimal")) {
      fieldType = ColumnData.FIELDTYPE_DECIMAL;
    } else if (type.length() == 0) {
      fieldType = ColumnData.FIELDTYPE_EMPTY;
    }

    return fieldType;
  }


  /**
   * test value length to see if it will fit
   * 
   * @param value
   * @return
   */
  @Deprecated
  public int testSizeOfValue(String value) {
    int resize = 0;
    if (columnType.contains("text")) {
      resize = testSize_Text(value);

    } else if (columnType.contains("varchar")) {
      resize = testSize_Varchar(value);
    } 
    return resize;
  }

  /**
   * test text, nothing to do here
   * 
   * @param value
   * @return
   */
  @Deprecated
  public int testSize_Text(String value) {
    // TODO - add the other types of text (length sizes?)
    // TODO - what types of text are there?
    return 0;
  }

  /**
   * test varchar length
   * 
   * @param value
   * @return
   */
  @Deprecated
  public int testSize_Varchar(String value) {
    int resize = 0;
    if (value.length() > lengthChar) {
      resize = value.length();
    }
    return resize;
  }




}