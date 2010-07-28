package org.gonevertical.dts.lib;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class StringUtil {

	private Logger logger = Logger.getLogger(StringUtil.class);

	/**
	 * find a match in a string
	 * 
	 * @param regex
	 * @param s
	 * @return
	 */
	public static boolean findMatch(String regex, String s) {
		if (regex == null | s == null) {
			return false;
		}
		
    if (regex != null && regex.contains(")") == false) {
      System.err.println("oops!!! - you forgot to use parentheses to catch a group"); 
    }
		
		boolean found = false;
		try {
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(s);
			found = m.find();
		} catch (Exception e) {
			System.out.println("findMatch: regex error");
			found = false;
		}
		return found;
	}

	public static String getValue(String regex, String value) {
		if (regex == null | value == null) {
			return null;
		}

    if (regex != null && regex.contains(")") == false) {
      System.err.println("oops!!! - you forgot to use parentheses to catch a group"); 
    }

		String v = null;
		try {
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(value);
			boolean found = m.find();
			if (found == true) {
				v = m.group(1);
			}
		} catch (Exception e) {
			System.out.println("findMatch: regex error (check to see if you have a (group)");
			e.printStackTrace();
		}

		return v;
	}

	public static String[] getValues(String regex, String value) {
		if (regex == null || value == null) {
			return null;
		}
		
		if (regex != null && regex.contains(")") == false) {
		  System.err.println("oops!!! - you forgot to use parentheses to catch a group"); 
		}

		ArrayList<String> s = new ArrayList<String>();
		try {
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(value);

			while (m.find()) {
				s.add(m.group(1));
			}

		} catch (Exception e) {
			System.out.println("findMatch: regex error (check to see if you have a (group)");
			e.printStackTrace();
		}

		if (s.size() == 0) {
			return null;
		}

		String[] r = new String[s.size()];
		s.toArray(r);

		return r;
	}

	/**
	 * string array to csv using now quotes
	 * @param s
	 * @return
	 */
	public static String toCsv_NoQuotes(String[] s) {
		if (s == null) {
			return null;
		}
		int l = s.length;
		String r = "";
		if (l == 1) { // only one
			r = s[0];
		} else {
			for (int i=0; i < s.length; i++) {
				r += s[i];
				if (i < s.length-1) {
					r += ",";
				}
			}
		}
		if (r != null && r.trim().length() == 0) {
			return null;
		}
		return r;
	}

	/**
	 * read sql set parameters
	 *	like a='b', b='xyz''123', c=null, d='aasdf''s''dasdf' 
	 *
	 * 
	 * @param sql
	 * @return
	 */
	public static String[] readSqlSplit(String sql) {

		if (sql == null) {
			return null;
		}

		ArrayList<String> p = new ArrayList<String>();
		String s = "";
		boolean encapsulate = false;
		int marker = 0;
		for (int i=0; i < sql.length(); i++) {

			char c = sql.charAt(i);

			String sc = Character.toString(c);

			//System.out.println("char: " + sc);

			if (encapsulate == true && marker == i-1) {
				//skip
				//System.out.println("\tskip");
			} else if (sc.equals("'") && encapsulate == false) {
				encapsulate = true;
				marker = i;
				//System.out.println("\tencapsulate = true");
			} else if (sc.equals("'") && encapsulate == true) {
				encapsulate = false;
				//System.out.println("\tencapsulate = false");
			}

			if (encapsulate == false && sc.equals(",")) {
				//System.out.println("\tadding " + s);
				p.add(s);
				s = "";
			} else {
				s += sc;
				//System.out.println("\t ssss:" + s);
			}

			//System.out.print("");
		}

		p.add(s);

		if (p.size() == 0) {
			return null;
		}

		String [] a = new String[p.size()];
		p.toArray(a);

		return a;
	}

}
