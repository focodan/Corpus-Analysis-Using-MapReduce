package ngrams;

import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramsReducer extends Reducer< Text , IntWritable , Text , Text > {
	
	public void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException , InterruptedException {

		// maybe I should use the context to avoid abuse and have better code?
		String fileName = (key.toString()).split("?")[0];
		String nGram = "\""+(key.toString()).split("?")[1]+"\"";
		
		int sum = 0;
		
		for(IntWritable ar : values){
			/*long[] curr = (long[])ar.get();
			totals[0] += curr[0];
			totals[1] += curr[1];
			totals[2] += curr[2];*/
			sum += ar.get() ;
		}

		/*//only to make formulas look more readable
		long totalSentences = totals[0];
		long totalWords = totals[1];
		long totalSyllables = totals[2];
		
		// these scores are calculated with magic numbers
		double fleschReadingEase = 206.835 - 1.015*(((double)totalWords)/totalSentences) 
									- 84.6*(((double)totalSyllables)/totalWords);
		double fleschKinCaidGrade = 0.39*(((double)totalWords)/totalSentences) 
									+ 11.8*(((double)totalSyllables)/totalWords) -15.59;
		
		String formattedResult = new String(key.toString()+" "+fleschReadingEase+", "+fleschKinCaidGrade);
		*/
		context.write(new Text(fileName),new Text(nGram+" "+sum));
	}
	
} //end reducer class