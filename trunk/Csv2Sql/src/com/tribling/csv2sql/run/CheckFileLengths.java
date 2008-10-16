package com.tribling.csv2sql.run;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public class CheckFileLengths {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		File file = new File("C:\\Archive\\parse\\long");
		
		File[] files  = file.listFiles();

		Arrays.sort(files);
		
		for (int i=0; i < files.length; i++ ){
			
			int totalRows = 0;
			if (files[i].isFile()) {
				totalRows = readFile(files[i]);
			}
			int i3 = (int) (totalRows - 3);
			
			// get length of file - how many rows -3
			System.out.println("" + files[i].getName() + " totalRows: " + totalRows + " -3: " + i3);
			
		}
	}
	
	private static int readFile(File file) {

		FileInputStream fis = null;
		BufferedInputStream bis = null;
		DataInputStream dis = null;

		int i = 0;
		try {
			fis = new FileInputStream(file);

			// Here BufferedInputStream is added for fast reading.
			bis = new BufferedInputStream(fis);
			dis = new DataInputStream(bis);

			// dis.available() returns 0 if the file does not have more lines.
			while (dis.available() != 0) {
				dis.readLine();
				//System.out.println(dis.readLine());
				i++;
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

		return i;
	}

}
