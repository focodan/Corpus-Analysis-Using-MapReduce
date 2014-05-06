package frequencies;

import util.Parser;
import java.io.IOException;
import java.util.ArrayList;

import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducerSlim extends Reducer< Text , TextPair , Text , Text > {

	///////changed to reduce1, so that the job will use the identity reducer.
	public void reduce(Text key, Iterable <TextPair> values, Context context) throws IOException , InterruptedException {

		//get rid of non-alphabet characters 
		String k = key.toString();
		k = Parser.removeNonPrintableChars(k);
		if(Parser.containsDigit(k)){
			return; // We won't write out numeric-words
		}
		//http://stackoverflow.com/questions/7161534/fastest-way-to-strip-all-non-printable-characters-from-a-java-string
		
		
		// this is used to extract our configuration value for N
		Configuration conf = context.getConfiguration();
		final int N = new Integer(conf.get("NumDocs")); 
		String docScoreList = ","; //to separate the key from the values list

		ArrayList<TextPair> docTf = new ArrayList<TextPair>();

		ArrayList<String> docIDs = new ArrayList<String>();
		ArrayList<String> TFs = new ArrayList<String>();
		
		for(TextPair pair : values){
			docTf.add(pair);
			docIDs.add(pair.getFirst().toString());
			TFs.add(pair.getSecond().toString());
			//docScoreList += pair;
		}
		
		
		double idf = Math.log10(((double)N)/docTf.size());
		
		for(int i=0;i<docTf.size();i++){
			//TextPair pair = docTf.get(i);
			String docID = docIDs.get(i);//pair.getFirst().toString();
			String tfScore = TFs.get(i);//pair.getSecond().toString();
			Double tfidf = TFIDF(idf,new Double(tfScore));
			if(idf>2.0 && idf<2.9){ // only important scoring words
				context.write(new Text(k),new Text(","+new Double(idf).toString()+","+docID));
				docScoreList += docID+","+TFIDF(idf,new Double(tfScore));

				if(i!=docTf.size()-1) { // separate intermediate pairs w/ comma
					docScoreList += ",";
				}
			}
			else{
				return;
			}
		}

		/*if(!docScoreList.equals(",")){
			context.write(key, new Text(docScoreList));
		}*/
	}
	private Double TFIDF(double idf, double tf){
		return idf*tf;
	}
	
} //end reducer class