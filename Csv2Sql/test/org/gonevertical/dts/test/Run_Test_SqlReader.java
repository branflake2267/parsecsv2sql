package org.gonevertical.dts.test;

import org.gonevertical.dts.lib.StringUtil;

public class Run_Test_SqlReader {
	
	public static void main(String[] args) {
		
		String value = "a='b', 12azc='1,adfda,11''22', d='4',e='abc''d , flr2',zz='e''n,d'";
		
		String regex = "(.*?=.*?'.+?'([\040]+)?)(,|$)";
		String[] as = StringUtil.getValues(regex, value);
		
		for (int i=0; i < as.length; i++) {
			System.out.println(i + ". " + as[i].trim());
		}
		
		System.out.println("end");
		
		
	}
	
}
