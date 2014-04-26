package ngrams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class NGramsMapper extends Mapper <LongWritable ,Text , Text , Text> {
	public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		// this is used to extract our configuration value for N
		Configuration conf = context.getConfiguration();
		final int N = new Integer(conf.get("Ngram")); 

		String section = value.toString();

		String[] sentences = section.split(".?!");
		ArrayList<String> validWords = new ArrayList<String>();

		
		for(String sentence: sentences){
			String[] words = sentence.split("[\\W]");
			validWords.clear();
			for(String w: words){
				boolean noDigits = true;
				for(int i=0;i<10;i++){
					String[] digits = {"0","1","2","3","4","5","6","7","8","9"};
					if(w.contains(digits[i])){
						noDigits = false; // I'm skipping numbers and words like "2nd"
					}
				}
				if(noDigits && !w.equals(" ")){ // a word I consider to be valid
					
					validWords.add(w.toLowerCase()); // flatten case
				}
			}
			// algorithm here
			// for each set of n elements of valid words
			//     write the joined n elements to the context
			for(int i=0; i+N <= validWords.size();i++){
				List<String> nGramList = validWords.subList(i,i+N);
				String nGram = "";
				for( String s : nGramList){
					nGram += s + " ";
				} nGram = nGram.substring(0, nGram.length()); // remove trailing " "
				// '?' is my delimiter b/c it's guarenteed to have already been stripped out.
				context.write(new Text(fileName/*fileName+" "+*//*nGram*/), new Text(nGram)); 
			}
		}
		// emit total to output collector
		//context.write(new Text(fileName)/*key*/, new ArrayPrimitiveWritable(new long[] {sentenceCount,wordCount,syllableCount}));
	}
	//end map
} //end mapper class