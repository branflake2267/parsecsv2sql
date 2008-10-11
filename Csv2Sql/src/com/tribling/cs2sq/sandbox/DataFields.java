package com.tribling.cs2sq.sandbox;

/**
 * object to store data in
 * 
 * @author branflake2267
 *
 */
public class DataFields implements Comparable<DataFields> {

	public String source;
	public String destination;
	
	@Override
	public int compareTo(DataFields o) {
		
		int i = this.source.compareTo(o.source);
		
		return i;
	}
	
}
