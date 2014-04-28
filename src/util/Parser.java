package util;

import java.util.ArrayList;
import java.util.List;


public class Parser {

	// Given a text section, split into a list of sentences
	public static String[] splitToSentences(String splitStr){
		String[] sentences = splitStr.split("\\?|\\!|\\.");
		return (sentences);
	}
	
	// Given a sentence, split into a list of 'valid' words
	public static ArrayList<String> sentenceToWords(String sentence){
		//ArrayList<String> rawWords = new ArrayList<String>();
		String[] rawWords = sentence.split(" +|,|;|\\(|\\)|:|\"");
		ArrayList<String> validWords = new ArrayList<String>();
		
		for(int i=0;i<rawWords.length;i++){
			//some test here
			if(rawWords[i].equals("")||rawWords[i].equals(" ")||rawWords[i].equals(",")
					||rawWords[i].equals(";")||rawWords[i].equals(":")||rawWords[i].equals("\"")
					||rawWords[i].equals(")")||rawWords[i].equals("(")){}
			else validWords.add(rawWords[i].toLowerCase());
		}
		
		return validWords;
	}
	
	// N-Gram building, here just as a quick testing place. This is now merged into NGramsMapper.java,
	// this can be removed at any time.
	/*public static ArrayList<String> nGramListT(ArrayList<String> words, int N){
		ArrayList<String> grams = new ArrayList<String>();
		for(int i=0; i+N <= words.size();i++){
			List<String> nGramList = words.subList(i,i+N);
			String nGram = "";
			for(int j=0;j<nGramList.size();j++){// String s : nGramList){
				nGram += nGramList.get(j);
				if(j!=nGramList.size()-1){ nGram+=" "; } // join all the middle words with a space
			}// nGram = nGram.substring(0, nGram.length()); // remove trailing " "
			grams.add(nGram);
		}
		return grams;
	}*/
	
	// Test area
	public static void main(String[] args){
		String testSplit = "I have been asked to write a few words of preface to this work. If the life-long friendship of my mother with her Majesty, which gained for me the honour of often seeing the Queen, or a deep feeling of loyalty and affection for our sovereign, which is shared by all her subjects, be accepted as a qualification, I gratefully respond to the call, but I feel that no written words of mine can add value to the following pages. This should be a sentence? Well yes! It should indeed.!?.";
		String[] testSplitAr = splitToSentences(testSplit);
		for(int i=0;i<testSplitAr.length;i++) System.out.println(testSplitAr[i]);
		
		System.out.println("------------------------------------------------------------");
		
		String testSentence = "if one were to split (and))  two-by-two, then I'd say \"hello\" 2nd to you too";
		
		ArrayList<String> words = sentenceToWords(testSentence);
		for( String w: words) System.out.println(":"+w+":");
		
		System.out.println("------------------------------------------------------------");
	
		/*ArrayList<String> grams = nGramListT(words,4);
		for (String g : grams) System.out.println(":"+g+":");*/
	}
	
}