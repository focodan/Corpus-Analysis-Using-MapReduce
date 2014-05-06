/**
 * Parsing utility class
 * Dan Elliott, 2014
 */

package util;

import java.util.ArrayList;
//import java.util.List;


public class Parser {

	// Given a text section, split into a list of sentences
	public static String[] splitToSentences(String splitStr){
		String[] sentences = splitStr.split("\\?|\\!|\\.");
		return (sentences);
	}
	
	// Given a sentence, split into a list of 'valid' words
	public static ArrayList<String> sentenceToWords(String sentence){
		//ArrayList<String> rawWords = new ArrayList<String>();
		String[] rawWords = sentence.split(" +|,|\\$|_|;|\\(|\\)|:|\"");
		ArrayList<String> validWords = new ArrayList<String>();

		for(int i=0;i<rawWords.length;i++){
			//some test here
			//rawWords[i] = rawWords[i].replaceAll("","");
			if(rawWords[i].equals("")||rawWords[i].equals(" ")||rawWords[i].equals(",")
					||rawWords[i].equals(";")||rawWords[i].equals(":")||rawWords[i].equals("\"")
					||rawWords[i].equals(")")||rawWords[i].equals("$")||rawWords[i].equals("_")
					||rawWords[i].equals("(")){}
			else validWords.add(rawWords[i].toLowerCase());
		}

		return validWords;
	}
	
	public static String removeNonPrintableChars(String s){
		int length = s.length();
		char[] oldChars = new char[length];
		s.getChars(0, length, oldChars, 0);
		int newLen = 0;
		for (int j = 0; j < length; j++) {
		    char ch = oldChars[j];
		    if (ch >= ' ' && ch <= '~') {
		        oldChars[newLen] = ch;
		        newLen++;
		    }
		}
		s = new String(oldChars, 0, newLen);
		return s;
	}

	public static boolean containsDigit(String s){
		String[] digits = {"0","1","2","3","4","5","6","7","8","9"};
		for( String d : digits){
			if(s.contains(d)) return true;
		}
		return false;
	}
	
	// Test area
	public static void main(String[] args){

	}
	
}
