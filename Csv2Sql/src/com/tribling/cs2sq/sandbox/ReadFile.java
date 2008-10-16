package com.tribling.csv2sql.sandbox;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class ReadFile {

	private int index = 0;
	private int row = 0;
	private int columnCount = 0;
	private String csvRow = "";
	
	private FileOutputStream fout;	
	
	public ReadFile() {

		
	}

	public void read(File file, int index) {
		this.index = index;
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		DataInputStream dis = null;

		try {
			fis = new FileInputStream(file);

			// Here BufferedInputStream is added for fast reading.
			bis = new BufferedInputStream(fis);
			dis = new DataInputStream(bis);

			// dis.available() returns 0 if the file does not have more lines.
			while (dis.available() != 0) {

				parseLine(dis.readLine());
			}

			// dispose all the resources after using them.
			fis.close();
			bis.close();
			dis.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
		
	private void parseLine(String l) {
		
		if (l.matches("^<TD.*?>.*?</TD>")) {
			getColumn(l);
		}
	}
	
	private void getColumn(String l) {

		Pattern p = Pattern.compile("<TD.*?>(.*?)</TD>");
		Matcher m = p.matcher(l);
		boolean found = m.find();

		if (found == true) {
			String f = m.group(1);
			columnCount++;
			
			if (columnCount < 3) {
				f += ",";
			}
			csvRow += f; 
		}
		
		if (columnCount == 3) {
			
			columnCount = 0;
			row++;
			
			
			writeLine(csvRow);
			
			// clear at end
			csvRow = "";
		}
		
	}

	
	public void writeOpen() {
	    try {
			fout = new FileOutputStream ("C:\\Archive\\parse\\vrepair2\\tickets2.csv");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void writeLine(String s) {
		new PrintStream(fout).println(s);
		System.out.println(index + ". " + row + ". " + s);
	}
	
	public void writeClose() {
	    try {
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
