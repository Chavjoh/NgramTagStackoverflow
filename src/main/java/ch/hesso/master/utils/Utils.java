package ch.hesso.master.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Utils {
	
	/**
	 * Utility to split a line of text in words.
	 *  
	 * @param text what we want to split
	 * @return words in text as an Array of String
	 */
	public static String[] words(String text) {
		String filteredText = text.toLowerCase();
	    StringTokenizer st = new StringTokenizer(filteredText);
	    ArrayList<String> result = new ArrayList<String>();
	    
	    while (st.hasMoreTokens()) {
    		result.add(st.nextToken());
	    }
	    
	    return Arrays.copyOf(result.toArray(),result.size(),String[].class);
	}
}
