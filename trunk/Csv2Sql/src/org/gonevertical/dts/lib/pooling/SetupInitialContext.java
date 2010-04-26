package org.gonevertical.dts.lib.pooling;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SetupInitialContext {
	
	private String contextPath = null;
	
	private InitialContext initalContext = null;

	public SetupInitialContext(String tmpPath) {
		
		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
		
		File d = new File(tmpPath + "/jdbc");
		d.mkdirs();
		
		System.setProperty(Context.PROVIDER_URL, "file://" + tmpPath + "");
		
    try {
    	initalContext = new InitialContext();
    } catch (NamingException e) {
	    e.printStackTrace();
    }
		
	}
	
	public void setContextXmlFileLocation(String contextXmlPath) {
		this.contextPath = contextXmlPath;
	}
	
	public void run() {
		
		// get the available resources via context.xml - same as tomcat resource xml
		ResourceData[] rds = getResources();
		
		for (int i=0; i < rds.length; i++) {
			
			String name = rds[i].getName();
			
			try {
	      initalContext.rebind(name, rds[i].getReference());
      } catch (NamingException e) {
	      e.printStackTrace();
      }
			
		}
		
	}

	private ResourceData[] getResources() {
		ResourceData[] rds = readXmlFile();		
	  return rds;
  }
	
	private ResourceData[] readXmlFile() {
		
		ResourceData[] rds = null;
		
		//get the factory
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(contextPath);
			Element docEle = dom.getDocumentElement();

			//get a nodelist of  elements
			NodeList nl = docEle.getElementsByTagName("Resource");

			if (nl != null && nl.getLength() > 0) {

				rds = new ResourceData[nl.getLength()];
				for (int i = 0 ; i < nl.getLength(); i++) {
					rds[i] = new ResourceData();
					
					Element element = (Element) nl.item(i);

					rds[i] = parseAttributes(element);
				}
			}

		}catch(ParserConfigurationException pce) {
			pce.printStackTrace();
		}catch(SAXException se) {
			se.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		
		return rds;
	}
	
	private ResourceData parseAttributes(Element element) {

		String resourceName = null;
		HashMap<String, String> hm = new HashMap<String, String>();
		
		NamedNodeMap at = element.getAttributes();
		for (int i=0; i < at.getLength(); i++) {

			Node n = at.item(i);
			String name = n.getNodeName();
			String value = n.getNodeValue();

			if (name.equals("name") == true) {
				resourceName = value;
			} else {
				hm.put(name, value);
			}

			//System.out.println("attribute: " + name + " " + value);
		}
		
		ResourceData rd = new ResourceData();
		rd.setName(resourceName);
		rd.setHashMap(hm);
		
		return rd;
	}

	public Context getInitialContext() {
	  return initalContext;
  }
	
}
