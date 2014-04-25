package readingScores;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileSplit;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class ReadingScores extends Configured implements Tool {

	
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {		
		private int countSyllables(String word)
	    {
	        char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
	        String currentWord = new String(word);
	        int numVowels = 0;
	        boolean lastWasVowel = false;
	        for (int i=0;i<currentWord.length();i++)//char wc : currentWord)
	        {
	        	char wc = currentWord.charAt(i);
	            boolean foundVowel = false;
	            for(int j=0;j<vowels.length;j++)// (char v in vowels)
	            {
	            	char v = vowels[j];
	                //don't count diphthongs
	                if (v == wc && lastWasVowel)
	                {
	                    foundVowel = true;
	                    lastWasVowel = true;
	                    break;
	                }
	                else if (v == wc && !lastWasVowel)
	                {
	                    numVowels++;
	                    foundVowel = true;
	                    lastWasVowel = true;
	                    break;
	                }
	            }
	 
	            //if full cycle and no vowel found, set lastWasVowel to false;
	            if (!foundVowel)
	                lastWasVowel = false;
	        }
	        //remove es, it's _usually? silent
	        if (currentWord.length() > 2 && 
	            currentWord.substring(currentWord.length() - 2).equals( "es"))
	            numVowels--;
	        // remove silent e
	        else if (currentWord.length() > 1 &&
	            currentWord.substring(currentWord.length() - 1).equals("e"))
	            numVowels--;

	        return numVowels;
	    }

		public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			long sentenceCount = 0;
			long wordCount = 0;
			long syllableCount = 0;
			
			//split text section into sentences
			//	size is sentence count
			
			//split text section into words
			//	add to wordCount
			
			//for each word, calculate syllable count, increment counter by that amount
			
			// emit key, array to output collector

			String section = value.toString();

			String[] sentences = section.split(".?!");
			sentenceCount = sentences.length;

			for(String sentence: sentences){
				String[] words = sentence.split("[\\W]");
				for(String w: words){
					boolean noDigits = true;
					for(int i=0;i<10;i++){
						String[] digits = {"0","1","2","3","4","5","6","7","8","9"};
						if(w.contains(digits[i])){
							noDigits = false; // I'm skipping numbers and words like "2nd"
						}
					}
					if(noDigits){ // a word I consider to be valid
						
						++wordCount;
						syllableCount += countSyllables(w);
						
						//word.set(w);
						//output.collect(word,one);
					}
				}
			}
			// emit total to output collector
			context.write(new Text(fileName)/*key*/, new ArrayPrimitiveWritable(new long[] {sentenceCount,wordCount,syllableCount}));
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), ReadingScores.class);
    conf.setJobName("readingScores");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class); // dpe change this
    
    conf.setMapperClass(MapClass.class);        
    //conf.setCombinerClass(Reduce.class); // dpe maybe not use combiner
    conf.setReducerClass(Reduce.class);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReadingScores(), args);
    System.exit(res);
  }

}
