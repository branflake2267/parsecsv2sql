package org.gonevertical.dts.data;

import java.io.File;

public class SourceData {
 
	// file or directory
	public File file;
	
	// column/field separator
	public char delimiter;
	
	// flate file pre-processing
	public FlatFileSettingsData ffsd = null;
	
	// subsitute header file
	private File header = null;

	// substitue header file, does the first row have fields or headers
	private boolean skipFirstRowBcOfSubHeaders = false;

	// substitue header file, header delimiter
	private char headerDelimiter;
	
	/**
	 * constructor
	 */
	public SourceData() {
	}
	
	/**
	 * is the source file a directory?
	 * @return
	 */
	public boolean isFileDirectory() {
	  boolean b = file.isDirectory();
	  return b;
	}
	
	/**
	 * change out the headers (columns / field names) with a file of your choice
	 * 
	 * @param header
	 * @param firstRowHasFields
	 */
	public void setSubstitueHeaders(File file, boolean skipFirstRowBcOfSubHeaders, char delimiter) {
		this.header = file;
		this.skipFirstRowBcOfSubHeaders = skipFirstRowBcOfSubHeaders;
		this.headerDelimiter = delimiter;
	}

	public File getHeadersFile() {
		return header;
	}
	
	public boolean getSkipFirstRowBcOfSubHeaders() {
		return skipFirstRowBcOfSubHeaders;
	}

	public char getHeaderDelimiter() {
	  return headerDelimiter;
  }
}
