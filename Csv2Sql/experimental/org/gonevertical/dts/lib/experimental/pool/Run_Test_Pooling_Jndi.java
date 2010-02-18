package org.gonevertical.dts.lib.experimental.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

public class Run_Test_Pooling_Jndi {


	public static void main(String[] args) {


		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
		
		System.setProperty(Context.PROVIDER_URL, "file:///tmp");
		
		InitialContext ic = null;
    try {
	    ic = new InitialContext();
    } catch (NamingException e) {
	    e.printStackTrace();
    }

		// Construct BasicDataSource reference
		Reference ref = new Reference("javax.sql.DataSource", "org.apache.commons.dbcp.BasicDataSourceFactory", null);
		ref.add(new StringRefAddr("driverClassName", "org.apache.commons.dbcp.TesterDriver"));
		ref.add(new StringRefAddr("url", "jdbc:apache:commons:testdriver"));
		ref.add(new StringRefAddr("username", "username"));
		ref.add(new StringRefAddr("password", "password"));
		
		try {
	    ic.rebind("jdbc/basic", ref);
    } catch (NamingException e) {

	    e.printStackTrace();
    }

		
		InitialContext ic2 = null;
    try {
	    ic2 = new InitialContext();
    } catch (NamingException e) {
	    e.printStackTrace();
    }
    
		DataSource ds = null;
    try {
	    ds = (DataSource) ic2.lookup("jdbc/basic");
    } catch (NamingException e) {
	    e.printStackTrace();
    }
		
		Connection conn = null;
    try {
	    conn = ds.getConnection();
    } catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
		
		try {
	    conn.close();
    } catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }

	}

}
