package org.gonevertical.dts.data;

import java.util.Comparator;

import org.apache.log4j.Logger;

public class SortDestinationField implements Comparator<FieldData> {

	private Logger logger = Logger.getLogger(SortDestinationField.class);
	
	@Override
	public int compare(FieldData a, FieldData b) {
		
		int i = a.destinationField.compareToIgnoreCase(b.destinationField);
		
		return i;
	}
	
}
