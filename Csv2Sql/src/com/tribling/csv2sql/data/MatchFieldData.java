package com.tribling.csv2sql.data;

/**
 * match source field and transform them into the destination field
 * 
 * RESERVED COLUMNS [ImportID, DateCreated, DateUpdated]
 * 
 * @author branflake2267
 *
 */
public class MatchFieldData implements Comparable<MatchFieldData> {
 
  // source field name
	public String sourceField;
	
	// destination field name
	public String destinationField;
	
	// when using identity insert index on a beginning type
	// you can force a column type for optimization of the field in the beginning
	// otherwise an identity column starts with varchar(50)
	public String destinationField_ColumnType = null;
	
	
	// when this field is used for transfer, and different data value appears overwrite? 
	private boolean overWriteOnUpdate = true;
	
  // used to set these field names as the identity values, or primary key values
  private boolean isIdentity = false;
	
	/**
	 * constructor
	 */
	public MatchFieldData() {
	}

	
	public int compareTo(MatchFieldData b) {
		
		if (sourceField == null) {
			return 0;
		}
		
		if (b == null) {
			return 0;
		}
		
		return sourceField.compareToIgnoreCase(b.sourceField);
	}

	

	
}
