package com.tribling.csv2sql.data;

import java.util.Comparator;

public class SortSourceField implements Comparator<MatchFieldData> {

	@Override
	public int compare(MatchFieldData a, MatchFieldData b) {
		
		int i = a.sourceField.compareToIgnoreCase(b.sourceField);
		
		return i;
	}

}
