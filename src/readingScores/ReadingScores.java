package readingScores;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadingScores {
	public class ScoreMapper extends Mapper <LongWritable ,Text , Text , IntWritable> {
		public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line);
			while(tok.hasMoreTokens()) {
				context.write(new Text(tok.nextToken()) , new IntWritable(1));
			}
		} //end map
	} //end mapper class


	public class ScoreReducer extends Reducer< Text , IntWritable , Text , IntWritable > {
		public void reduce(Text key, Iterable <IntWritable> values, Context context ) throws IOException , InterruptedException {
			int sum = 0;
			for( IntWritable i : values ) {
				sum += i.get();
			}
			context.write( key , new IntWritable( sum ));
		} //end reduce
	}//end reducer class

	public static void main( String [] args ) throws IOException ,
	ClassNotFoundException,InterruptedException {
		Job job = Job.getInstance (new Configuration ());
		job.setJarByClass(ReadingScores.class); //this class’s name
		job.setJobName("Word Count"); //name of this job.
		FileInputFormat.addInputPath( job ,new Path( args [0])); //input path
		FileOutputFormat.setOutputPath( job ,new Path( args [1])); //output path
		job.setMapperClass(ScoreMapper.class ); //mapper class
		job.setCombinerClass( ScoreReducer.class ); //optional dpe perhaps remove
		job.setReducerClass( ScoreReducer.class ); //reducer class
		job.setOutputKeyClass( Text.class ); // the key your reducer outputs
		job.setOutputValueClass( IntWritable.class ); // the value
		System.exit( job.waitForCompletion( true ) ? 0 : 1);
	}
}
