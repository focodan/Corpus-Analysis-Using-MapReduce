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
		Job job = Job.getInstance (new Configuration ());
		job.setJarByClass(ReadingScores.class); //this class’s name
		job.setJobName("Word Count"); //name of this job.
		
		FileInputFormat.addInputPath( job ,new Path( args [0])); //input path
		FileOutputFormat.setOutputPath( job ,new Path( args [1])); //output path
		
		job.setMapperClass(ScoreMapper.class ); //mapper class
		//job.setCombinerClass( ScoreReducer.class ); //optional dpe perhaps remove
		job.setReducerClass( ScoreReducer.class ); //reducer class
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
		
		job.setOutputKeyClass( Text.class ); // the key your reducer outputs
		job.setOutputValueClass(Text.class ); // the value
		
		System.exit( job.waitForCompletion( true ) ? 0 : 1);
	}
}