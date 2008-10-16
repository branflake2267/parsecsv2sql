package com.tribling.csv2sql.sandbox;

import java.util.Arrays;
import java.util.Comparator;

public class RunSort {

	public static void main(String[] args) {
		
		DataFields[] fieldsData = new DataFields[4];
		fieldsData[0] = new DataFields();
		fieldsData[1] = new DataFields();
		fieldsData[2] = new DataFields();
		fieldsData[3] = new DataFields();
		
		fieldsData[0].source = "Ticket Number";
		fieldsData[0].destination = "TTN";
		
		fieldsData[1].source = "Employee ID";
		fieldsData[1].destination = "EMPID";
		
		fieldsData[2].source = "Survey";
		fieldsData[2].destination = "SurveyID";
		
		fieldsData[3].source = "Apple";
		fieldsData[3].destination = "Orange";
		
		// comparator to sort by
		Comparator<DataFields> sortBySource = new SortBySource();
		
		// sort it
		Arrays.sort(fieldsData, sortBySource);
		
		for(int i=0; i < fieldsData.length; i++) {
			System.out.println(i + ". " + fieldsData[i].source);
		}
		
		// comparator
		SortBySource searchComparator = new SortBySource();
		
		// This is what I am searching for
		DataFields searchFieldObject = new DataFields();
		searchFieldObject.source = "Survey";
		
		// this is how to find it
		int index = Arrays.binarySearch(fieldsData, searchFieldObject, searchComparator);
		
		System.out.println(searchFieldObject.source + " Found @ Index: " + index);
		
	}
	
}
