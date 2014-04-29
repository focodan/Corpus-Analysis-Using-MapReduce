package frequencies;
import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



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
		
		String[] sections = (value.toString()).split("\n");
		//
		//for(String line : sections){
		for(int i=0;i<sections.length;i++){
			String line = sections[i];
			String[] fields = line.split(",");
			//docname,"+ngram+","+count+","+tf(count,maxFrequency)))
			Text nGram = new Text(fields[1].trim());
			TextPair docTf = new TextPair(fields[0].trim(),fields[3].trim());
			context.write(nGram,docTf);
		}
	}
	//end map

} //end mapper class