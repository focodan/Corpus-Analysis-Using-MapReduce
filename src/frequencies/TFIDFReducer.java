package frequencies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer< Text , TextPair , Text , Text > {

	public void reduce(Text key, Iterable <TextPair> values, Context context) throws IOException , InterruptedException {

		// this is used to extract our configuration value for N
		Configuration conf = context.getConfiguration();
		final int N = new Integer(conf.get("NumDocs")); 
		
		// my key is a particular nGram
		// my values are all of the <doc,tf score> pairs.
		
		
		ArrayList<TextPair> docTf = new ArrayList<TextPair>();
		for(TextPair pair : values){
			docTf.add(pair);
		}
		
		String docScoreList = ","; //to separate the key from the values list
		double idf = Math.log10(((double)N)/docTf.size());
		for(int i=0;i<docTf.size();i++){
			TextPair pair = docTf.get(i);
			docScoreList += pair.getFirst()+","+TFIDF(idf,new Double(pair.getSecond().toString()));
			if(i!=docTf.size()-1) docScoreList += ",";
		}
		
		context.write(key, new Text(docScoreList));
	}
	private String TFIDF(Double idf, Double tf){
		return (new Double(idf*tf)).toString();
	}
	
} //end reducer class