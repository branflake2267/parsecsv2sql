package com.tribling.csv2sql.data;

import java.util.Comparator;
 
public class SortSourceField implements Comparator<FieldData> {

	@Override
	public int compare(FieldData a, FieldData b) {
		
		int i = a.sourceField.compareToIgnoreCase(b.sourceField);
		
		return i;
	}

}
