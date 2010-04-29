package org.gonevertical.dts.lib.pooling;

import java.util.HashMap;
import java.util.Iterator;

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.apache.log4j.Logger;

public class ResourceData {
	
	private Logger logger = Logger.getLogger(ResourceData.class);

	private String name = null;
	
	private HashMap<String, String> map = null;

	public void setName(String name) {
	  this.name = name;
  }

	public void setHashMap(HashMap<String, String> map) {
	  this.map = map;
  }
	
	public String getName() {
		return name;
	}
	
	public Reference getReference() {
		
		Reference ref = new Reference("javax.sql.DataSource", "org.apache.commons.dbcp.BasicDataSourceFactory", null);
		
		Iterator<String> itr = map.keySet().iterator();
		while (itr.hasNext()) {
			
			String name = itr.next();
			String value = map.get(name);
			
			StringRefAddr addr = new StringRefAddr(name, value);
			
			ref.add(addr);
		}
		
		return ref;
	}
	
	
}
