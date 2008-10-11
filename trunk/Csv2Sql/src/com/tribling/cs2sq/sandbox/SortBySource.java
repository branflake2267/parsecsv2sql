package com.tribling.cs2sq.sandbox;

import java.util.Comparator;

/**
 * use this interface to sort and search
 * 
 * @author branflake2267
 *
 */
public class SortBySource implements Comparator<DataFields> {

	@Override
	public int compare(DataFields a, DataFields b) {
	
		int i = a.source.compareTo(b.source);
		
		return i;
	}
}
