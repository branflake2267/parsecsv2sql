package org.gonevertical.csv2sql.data;

import java.util.Comparator;
 
public class FieldDataComparator implements Comparator<FieldData> {

  // sort by sourceField
	public int compare(FieldData a, FieldData b) {
		int i = a.sourceField.compareToIgnoreCase(b.sourceField);
		return i;
	}

}
