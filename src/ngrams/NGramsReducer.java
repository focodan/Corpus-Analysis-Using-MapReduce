package ngrams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramsReducer extends Reducer< Text , Text , Text , Text > {
	
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException , InterruptedException {

		HashMap<String,Integer> dictionary = new HashMap<>();
		int maxFrequency = 0; //I'll need the max frequency of an n-gram per document to calculate TF.
							// So, I may as well get it "for free" in this reducer.

		for( Text v : values){
			String val = v.toString();
			if(dictionary.containsKey(val)){ //increment recurrences
				dictionary.put(val, dictionary.get(val)+1);
			}
			else{ //add new value
				dictionary.put(val, new Integer(1));
			}
		}
		for (Entry<String, Integer> entry : dictionary.entrySet()) { //TODO simplify this
		    //String ngram = entry.getKey();
		    Integer count = entry.getValue();
		    if(count > maxFrequency){
		    	maxFrequency = count;
		    }
		}
		for (Entry<String, Integer> entry : dictionary.entrySet()) {
		    String ngram = entry.getKey();
		    Integer count = entry.getValue();
		    // The final output will be: <file name> ,<ngram>,<frequency>,<tfScore> // CSV like a champ!
		    context.write(key,new Text(","+ngram+","+count+","+tf(count,maxFrequency)));
		}
	}
	
	private double tf(int termCount,int maxFrequency){
		return 0.5 + (0.5*termCount)/((double)maxFrequency);
	}
	
} //end reducer class