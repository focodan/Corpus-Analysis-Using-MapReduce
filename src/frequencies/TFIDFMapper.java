package frequencies;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
		/*String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		String section = value.toString();

		String[] sentences = Parser.splitToSentences(section);
		ArrayList<String> validWords = new ArrayList<String>();

		
		for(String sentence: sentences){
			validWords = Parser.sentenceToWords(sentence);
			ArrayList<String> grams = nGramList(validWords,N);
			for(String g : grams){
				context.write(new Text(fileName), new TextPair(g,g));
			}
		}*/
		//HashMap<String,ArrayList<TextPair>> ngramVectors;
		
		String[] sections = (value.toString()).split("\n\r");
		//try{
		for(String line : sections){
			String[] fields = line.split(",");
			//docname,"+ngram+","+count+","+tf(count,maxFrequency)))
			Text nGram = new Text(fields[1]);
			TextPair docTf = new TextPair(fields[0],fields[3]);
			context.write(nGram,docTf);
			
		}
		


	}
	//end map

} //end mapper class