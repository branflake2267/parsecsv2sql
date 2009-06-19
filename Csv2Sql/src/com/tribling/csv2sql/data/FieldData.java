package com.tribling.csv2sql.data;

import java.util.HashMap;

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
	
	// Transfer: this designates this column is used as the primary key
	public boolean isPrimaryKey = false;
	
	// Transfer: on copy when dest value is blank
	public boolean onlyOverwriteBlank = true;
	
	// Transfer: only take this from the source field
	public String regexSourceField = null;
	
	// Transfer: different destination table for this value
	public String differentDestinationTable = null;
	
	// Transfer: different destination table hard code column + values - one to many definition
	public HashMap<String, String> hardOneToMany = null; // new HashMap<String, String>(); 
	
	/**
	 * constructor
	 */
	public FieldData() {
	}

	/**
	 * sort the fields by source field
	 */
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
