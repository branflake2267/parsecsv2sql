package com.tribling.csv2sql.data;

public class MatchFieldData implements Comparable<MatchFieldData> {

	public String sourceField;
	public String desinationField;
	
	/**
	 * constructor
	 */
	public MatchFieldData() {
	}

	@Override
	public int compareTo(MatchFieldData b) {
		return sourceField.compareToIgnoreCase(b.sourceField);
	}

	

	
}
