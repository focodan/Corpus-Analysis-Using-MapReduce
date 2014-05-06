package readingScores;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadingScores {
	public static void main( String [] args ) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance (conf);
		job.setJarByClass(ReadingScores.class); //this class’s name
		job.setJobName("Flesch Scores"); //name of this job.
		
		FileInputFormat.addInputPath( job ,new Path( args [0])); //input path
		FileOutputFormat.setOutputPath( job ,new Path( args [1])); //output path
		
		job.setMapperClass(ScoreMapper.class ); //mapper class
		//job.setCombinerClass( ScoreReducer.class ); //optional dpe perhaps remove
		job.setReducerClass( ScoreReducer.class ); //reducer class
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
		
		job.setOutputKeyClass( Text.class ); // the key your reducer outputs
		job.setOutputValueClass(Text.class ); // the value
		
		//Test for compressed output
		FileOutputFormat.setCompressOutput(job, true);
	    FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
	    
		
		System.exit( job.waitForCompletion( true ) ? 0 : 1);
	}
}