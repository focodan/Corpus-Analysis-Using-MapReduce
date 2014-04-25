package readingScores;

import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper <LongWritable ,Text , Text , ArrayPrimitiveWritable> {
	public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		long sentenceCount = 0;
		long wordCount = 0;
		long syllableCount = 0;

		//split text section into sentences
		// size is sentence count

		//split text section into words
		// add to wordCount

		//for each word, calculate syllable count, increment counter by that amount

		// emit key, array to output collector

		String section = value.toString();

		String[] sentences = section.split(".?!");
		sentenceCount = sentences.length;

		for(String sentence: sentences){
			String[] words = sentence.split("[\\W]");
			for(String w: words){
				boolean noDigits = true;
				for(int i=0;i<10;i++){
					String[] digits = {"0","1","2","3","4","5","6","7","8","9"};
					if(w.contains(digits[i])){
						noDigits = false; // I'm skipping numbers and words like "2nd"
					}
				}
				if(noDigits){ // a word I consider to be valid

					++wordCount;
					syllableCount += countSyllables(w);

					//word.set(w);
					//output.collect(word,one);
				}
			}
		}
		// emit total to output collector
		context.write(new Text(fileName)/*key*/, new ArrayPrimitiveWritable(new long[] {sentenceCount,wordCount,syllableCount}));
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