package org.gonevertical.dts.lib.experimental.pool;

import org.gonevertical.dts.lib.pooling.SetupInitialContext;

public class Run_Test_InitalContext {

	public static void main(String[] args) {
		
		String contextXmlPath = "/Users/branflake2267/Documents/workspace/Metrics_Nca/war/META-INF/context.xml";
		
		SetupInitialContext ic = new SetupInitialContext();
		ic.setContextXmlFileLocation(contextXmlPath);
		ic.run();
		
		
	}
	
}
