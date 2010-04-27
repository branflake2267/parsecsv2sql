package org.gonevertical.dts.v2;

import org.apache.log4j.Logger;

/**
 * run this from a parent project, not in standalone
 * 
 * @author branflake2267
 *
 */
public class Run_Logger_Test {

	private Logger logger = Logger.getLogger(FileProcessing_v2.class);
		
	// try including this in parent project
	public Run_Logger_Test() {
		
	}
	
	public void run() {
		
		logger.error("Test worked");
	}
	
}
