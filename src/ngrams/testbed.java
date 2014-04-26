package ngrams;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class testbed {

	
	
	public static void main(String[] args){
		int N = 3;
		ArrayList<String> validWords = new ArrayList<String>();
		
		String[] words = "the jive cat dances    swiftly".split(" +");
		
	    for(String w : words){
	    	System.out.println(":"+w);if(w.equals(" ")){}
	    	else validWords.add(w);
	    }
		
		for(int i=0; i+N <= validWords.size();i++){
			List<String> nGramList = validWords.subList(i,i+N);
			String nGram = "";
			for(int j=0;j<nGramList.size();j++){// String s : nGramList){
				nGram += nGramList.get(j);
				if(j!=nGramList.size()-1){ nGram+=" "; }
			}// nGram = nGram.substring(0, nGram.length()); // remove trailing " "
			// '?' is my delimiter b/c it's guarenteed to have already been stripped out.
			//context.write(new Text(fileName/*fileName+" "+*//*nGram*/), new Text(nGram));
			
			System.out.println(nGram);
		}
	}
}
