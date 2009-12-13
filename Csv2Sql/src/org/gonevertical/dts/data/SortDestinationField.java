package org.gonevertical.dts.data;

import java.util.Comparator;

public class SortDestinationField implements Comparator<FieldData> {

	@Override
	public int compare(FieldData a, FieldData b) {
		
		int i = a.destinationField.compareToIgnoreCase(b.destinationField);
		
		return i;
	}
	
}
