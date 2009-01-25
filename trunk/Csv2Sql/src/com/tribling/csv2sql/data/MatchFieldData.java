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
 
  // when finding this field in source
	public String sourceField;
	
	// change it to this field
	public String destinationField;
	
	// when using identity insert index on a beginning type
	// you can force a column type for optimization of the field in the beginning
	// otherwise an identity column starts with varchar(50)
	public String destinationField_ColumnType = null;
	
	
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
