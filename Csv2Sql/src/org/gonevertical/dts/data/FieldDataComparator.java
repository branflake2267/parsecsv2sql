package org.gonevertical.dts.data;

import java.util.Comparator;

import org.apache.log4j.Logger;
 
public class FieldDataComparator implements Comparator<FieldData> {
	
	private Logger logger = Logger.getLogger(FieldDataComparator.class);

  // sort by sourceField
	public int compare(FieldData a, FieldData b) {
		int i = a.sourceField.compareToIgnoreCase(b.sourceField);
		return i;
	}

}
