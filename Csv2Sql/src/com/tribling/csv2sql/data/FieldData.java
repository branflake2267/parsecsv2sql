package com.tribling.csv2sql.data;

/**
 * match source field and transform them into the destination field
 * 
 * RESERVED COLUMNS [ImportID, DateCreated, DateUpdated]
 * 
 * @author branflake2267
 *
 */
public class FieldData implements Comparable<FieldData> {
 
  // source field name
	public String sourceField;
	
	// destination field name
	public String destinationField;
	
	// when using identity insert index on a beginning type
	// you can force a column type for optimization of the field in the beginning
	// otherwise an identity column starts with varchar(50)
	public String destinationField_ColumnType = null;
	
	
	// Transfer: on copy when dest value is blank
	private boolean onlyOverwriteBlank = true;
	
	/**
	 * constructor
	 */
	public FieldData() {
	}

	
	public int compareTo(FieldData b) {
		
		if (sourceField == null) {
			return 0;
		}
		
		if (b == null) {
			return 0;
		}
		
		return sourceField.compareToIgnoreCase(b.sourceField);
	}

	

	
}
