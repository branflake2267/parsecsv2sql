package com.tribling.cs2sq.sandbox;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class RunSort {

	public static void main(String[] args) {
		
		DataFields[] fields = new DataFields[4];
		fields[0] = new DataFields();
		fields[1] = new DataFields();
		fields[2] = new DataFields();
		fields[3] = new DataFields();
		
		fields[0].source = "Ticket Number";
		fields[0].destination = "TTN";
		
		fields[1].source = "Employee ID";
		fields[1].destination = "EMPID";
		
		fields[2].source = "Survey";
		fields[2].destination = "SurveyID";
		
		fields[3].source = "Apple";
		fields[3].destination = "Orange";
		
		// comparator to sort by
		Comparator<DataFields> sortBySource = new SortBySource();
		
		// sort it
		Arrays.sort(fields, sortBySource);
		
		for(int i=0; i < fields.length; i++) {
			System.out.println(i + ". " + fields[i].source);
		}
		
		// comparator
		SortBySource search = new SortBySource();
		
		// This is what I am searching for
		DataFields searchField = new DataFields();
		searchField.source = "Survey";
		
		// this is how to find it
		int index = Arrays.binarySearch(fields, searchField, search);
		
		System.out.println("Index: " + index);
		
	}
	
}
