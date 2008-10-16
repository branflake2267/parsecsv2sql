package com.tribling.csv2sql.sandbox;

import java.io.File;

/**
 * parse an HTML Table to CSV
 * @author BDonnelson
 *
 */
public class HtmlTableParse {

	private static ReadFile read;

	public static void main(String[] args) {
		
		read = new ReadFile();
		
		run();
		
		System.out.println("The End");
	}
	
	public static void run() {
		File file = new File("C:\\Archive\\parse\\vrepair2");
		
		// get list of files in directory
		File[] files = file.listFiles();
		
		loop(files);
	}
	
	private static void loop(File[] files) {
		
		read.writeOpen();
		
		
		for (int i=0; i < files.length; i++) {
			if (files[i].isFile() == true) {
				read.read(files[i], i);
			}
		}
		
		read.writeClose();
		
		System.out.println("done reading files");

	}


	

}
