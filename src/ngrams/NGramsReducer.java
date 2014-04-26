package ngrams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramsReducer extends Reducer< Text , Text , Text , Text > {
	
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException , InterruptedException {

		HashMap<String,Integer> dictionary = new HashMap<>();
		
		for( Text v : values){
			String val = v.toString();
			if(dictionary.containsKey(val)){ //increment recurrences
				dictionary.put(val, dictionary.get(val)+1);
			}
			else{ //add new value
				dictionary.put(val, new Integer(1));
			}
		}
		for (Entry<String, Integer> entry : dictionary.entrySet()) {
		    String ngram = entry.getKey();
		    Integer count = entry.getValue();
		    context.write(key,new Text(ngram+" "+count));
		    // ...
		}
		//context.write(key,new Text());
		
//		// maybe I should use the context to avoid abuse and have better code?
//		/*String fileName = (key.toString()).split("\\?")[0];
//		String nGram = "\""+(key.toString()).split("\\?")[1]+"\"";
//		*/
//		
//		String nGram = key.toString();
//		
//		int sum = 0;
//		
//		for(IntWritable ar : values){
//			/*long[] curr = (long[])ar.get();
//			totals[0] += curr[0];
//			totals[1] += curr[1];
//			totals[2] += curr[2];*/
//			sum += ar.get() ;
//		}
//
//		/*//only to make formulas look more readable
//		long totalSentences = totals[0];
//		long totalWords = totals[1];
//		long totalSyllables = totals[2];
//		
//		// these scores are calculated with magic numbers
//		double fleschReadingEase = 206.835 - 1.015*(((double)totalWords)/totalSentences) 
//									- 84.6*(((double)totalSyllables)/totalWords);
//		double fleschKinCaidGrade = 0.39*(((double)totalWords)/totalSentences) 
//									+ 11.8*(((double)totalSyllables)/totalWords) -15.59;
//		
//		String formattedResult = new String(key.toString()+" "+fleschReadingEase+", "+fleschKinCaidGrade);
//		*/
//		context.write(/*new Text(fileName)*/key,new Text(/*nGram+" "+*/new Integer(sum).toString()));
	}
	
} //end reducer class