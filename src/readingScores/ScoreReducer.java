package readingScores;

import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer< Text , ArrayPrimitiveWritable , Text , Text > {
	
	public void reduce(Text key, Iterable <ArrayPrimitiveWritable> values, Context context) throws IOException , InterruptedException {

		long[] totals = new long[3]; // the sum of mapper outputs

		for(ArrayPrimitiveWritable ar : values){
			long[] curr = (long[])ar.get();
			totals[0] += curr[0];
			totals[1] += curr[1];
			totals[2] += curr[2];
		}

		//only to make formulas look more readable
		long totalSentences = totals[0];
		long totalWords = totals[1];
		long totalSyllables = totals[2];
		
		// these scores are calculated with magic numbers
		double fleschReadingEase = 206.835 - 1.015*(((double)totalWords)/totalSentences) 
									- 84.6*(((double)totalSyllables)/totalWords);
		double fleschKinCaidGrade = 0.39*(((double)totalWords)/totalSentences) 
									+ 11.8*(((double)totalSyllables)/totalWords) -15.59;
		
		String formattedResult = new String(/*key.toString()+" "+*/fleschReadingEase+", "+fleschKinCaidGrade);
		
		context.write(key,new Text(formattedResult));
	}
	
} //end reducer class