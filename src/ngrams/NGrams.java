package ngrams;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// have this class accept commandline arg for n-value, learn how context variables work

public class NGrams {
	public static void main( String [] args ) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		if(args.length >= 3){ // if user passed us a parameter explicitly
			conf.set("Ngram", args[2]); // dpe do something similar to set N value for N-gram
		}
		else{ // We default to 1-grams, or more simply, word count
			conf.set("Ngram", "1");
		}
		
		Job job = Job.getInstance (conf/*new Configuration ()*/);
		job.setJarByClass(NGrams.class); //this class’s name
		job.setJobName("N-Grams"); //name of this job.
		
		FileInputFormat.addInputPath( job ,new Path( args [0])); //input path
		FileOutputFormat.setOutputPath( job ,new Path( args [1])); //output path
		
		job.setMapperClass(NGramsMapper.class ); //mapper class
		//job.setCombinerClass( ScoreReducer.class ); //optional dpe perhaps remove
		job.setReducerClass( NGramsReducer.class ); //reducer class
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
		
		job.setOutputKeyClass( Text.class ); // the key your reducer outputs
		job.setOutputValueClass(Text.class ); // the value
		
		System.exit( job.waitForCompletion( true ) ? 0 : 1);
	}
}