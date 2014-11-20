package WordCount;

import java.io.File;

import MapReduce.MapReduceInterface;
import MapReduce.MapReducer;
import MapReduce.MapReducerConfig;
import Util.Host;
import Util.Tuple;

/**
 * Created by karansharma on 11/15/14.
 */
public class WordCount {

	public static void main(String[] args){
		MapReducerConfig config = new MapReducerConfig();
		config.setMapperClass(WordCountMap.class);
		config.setReducerClass(WordCountReduce.class);
//		config.setInputFormat(WordInputFormat.class);
//		config.setOutputFromat(WordOutputFormat.class);
		config.setInputFile("TropicThunderQuote");
		config.setOutputFile("WCOutput");
		File tropicThunder = new File("TropicThunderQuote");
		File[] files = new File[1];
		files[0] = tropicThunder;
		MapReducer map_reducer = null;
		try {
			map_reducer = new MapReducer("WordcountClient",new Host("Arjuns-MacBook-Pro-2",8080));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		try {
			map_reducer.runJob(config, files);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
