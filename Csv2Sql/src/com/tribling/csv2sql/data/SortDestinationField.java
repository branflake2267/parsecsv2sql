package com.tribling.csv2sql.data;

import java.util.Comparator;

public class SortDestinationField implements Comparator<MatchFieldData> {

	@Override
	public int compare(MatchFieldData a, MatchFieldData b) {
		
		int i = a.destinationField.compareToIgnoreCase(b.destinationField);
		
		return i;
	}
	
}
