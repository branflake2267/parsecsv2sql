package com.tribling.csv2sql.data;

public class MatchFieldData implements Comparable<MatchFieldData> {
 
	public String sourceField;
	public String desinationField;
	
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
