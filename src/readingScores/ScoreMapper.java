package readingScores;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import parseUtil.Parser;

public class ScoreMapper extends Mapper <LongWritable ,Text , Text , ArrayPrimitiveWritable> {
	public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		long sentenceCount = 0;
		long wordCount = 0;
		long syllableCount = 0;

		String section = value.toString();

		String[] sentences = Parser.splitToSentences(section);
		sentenceCount = sentences.length;

		for(String sentence: sentences){
			ArrayList<String> words = Parser.sentenceToWords(sentence);
			for(String w: words){
				++wordCount;
				syllableCount += countSyllables(w);
			}
			
		}
		// emit total to output collector
		context.write(new Text(fileName), new ArrayPrimitiveWritable(new long[] {sentenceCount,wordCount,syllableCount}));
	}
	//end map

	private int countSyllables(String word){
		char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
		String currentWord = new String(word);
		int numVowels = 0;
		boolean lastWasVowel = false;
		for (int i=0;i<currentWord.length();i++){
			char wc = currentWord.charAt(i);
			boolean foundVowel = false;
			for(int j=0;j<vowels.length;j++)
			{
				char v = vowels[j];
				//don't count diphthongs
				if (v == wc && lastWasVowel)
				{
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
				else if (v == wc && !lastWasVowel)
				{
					numVowels++;
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
			}

			//if full cycle and no vowel found, set lastWasVowel to false;
			if (!foundVowel)
				lastWasVowel = false;
		}
		//remove es, it's _usually? silent
		if (currentWord.length() > 2 &&
				currentWord.substring(currentWord.length() - 2).equals( "es"))
			numVowels--;
		// remove silent e
		else if (currentWord.length() > 1 &&
				currentWord.substring(currentWord.length() - 1).equals("e"))
			numVowels--;

		return numVowels;
	}
} //end mapper class