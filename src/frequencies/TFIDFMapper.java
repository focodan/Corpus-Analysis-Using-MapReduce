package frequencies;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import util.Parser;


public class TFIDFMapper extends Mapper <LongWritable ,Text , Text , TextPair> {
	public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		// this is used to extract our configuration value for N
		Configuration conf = context.getConfiguration();
		final int N = new Integer(conf.get("Ngram")); 

		String section = value.toString();

		String[] sentences = Parser.splitToSentences(section);
		ArrayList<String> validWords = new ArrayList<String>();

		
		for(String sentence: sentences){
			validWords = Parser.sentenceToWords(sentence);
			ArrayList<String> grams = nGramList(validWords,N);
			for(String g : grams){
				context.write(new Text(fileName), new TextPair(g,g));
			}
		}
	}
	//end map

	// nGram generator
	private ArrayList<String> nGramList(ArrayList<String> words, int N){
		ArrayList<String> grams = new ArrayList<String>();
		for(int i=0; i+N <= words.size();i++){
			List<String> nGramList = words.subList(i,i+N);
			String nGram = "";
			for(int j=0;j<nGramList.size();j++){
				nGram += nGramList.get(j);
				if(j!=nGramList.size()-1){ nGram+=" "; } // join all the middle words with a space
			}
			grams.add(nGram);
		}
		return grams;
	}

} //end mapper class