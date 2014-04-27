package ngrams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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
		}
	}
	
} //end reducer class