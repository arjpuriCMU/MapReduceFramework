package WordCount;

import MapReduce.MapReduceInterface;
import MapReduce.MapReducerConfig;
import Util.Tuple;

/**
 * Created by karansharma on 11/15/14.
 */
public class WordCount {

	public static void main(String args[]){
		MapReducerConfig config = new MapReducerConfig();
		config.setMapperClass(WordCountMap.class);
		config.setReducerClass(WordCountReduce.class);
		config.setInputFormat(WordInputFormat.class);
		config.setOutputFromat(WordOutputFormat.class);
		config.setInputFile("TropicThunderQuote");
		config.setOutputFile("WCOutput");
	}
}
