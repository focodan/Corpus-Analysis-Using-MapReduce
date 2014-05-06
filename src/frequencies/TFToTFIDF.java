package frequencies;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// have this class accept commandline arg for n-value, learn how context variables work

public class TFToTFIDF {
	public static void main( String [] args ) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		// set configuration variable for the number of documents in the corpus
		if(args.length >= 3){ // if user passed us a parameter explicitly
			conf.set("NumDocs", args[2]);
		}
		else{ // We default to 1 if left unspecified
			conf.set("NumDocs", "1");
		}
		
		Job job = Job.getInstance (conf/*new Configuration ()*/);
		job.setJarByClass(TFToTFIDF.class); //this class’s name
		job.setJobName("TF-IDF"); //name of this job.
		
		FileInputFormat.addInputPath( job ,new Path( args [0])); //input path
		FileOutputFormat.setOutputPath( job ,new Path( args [1])); //output path

		job.setMapperClass(TFIDFMapper.class); //mapper class
		//job.setCombinerClass( ScoreReducer.class ); //optional dpe perhaps remove
		job.setReducerClass(TFIDFReducerSlim.class); //reducer class

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);

		job.setOutputKeyClass( Text.class ); // the key your reducer outputs
		job.setOutputValueClass(Text.class ); // the value // change to arraywritable if i have enough time to re-style my code

		//Test for compressed output
		//FileOutputFormat.setCompressOutput(job, true);
		//FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);

		System.exit( job.waitForCompletion( true ) ? 0 : 1);
	}
}