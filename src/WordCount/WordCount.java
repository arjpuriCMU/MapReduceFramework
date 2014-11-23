package WordCount;

import java.io.File;
import java.io.IOException;

import MapReduce.MapReducer;
import MapReduce.MapReducerConfig;
import Util.FileFunctions;
import Util.Host;
import Util.Tuple;

/**
 * Created by karansharma on 11/15/14.
 */
public class WordCount {

	public static void main(String[] args) throws IOException{
		MapReducerConfig config = new MapReducerConfig();
		config.setMapperClass(WordCountMap.class);
		config.setReducerClass(WordCountReduce.class);
//		config.setInputFormat(WordInputFormat.class);
//		config.setOutputFromat(WordOutputFormat.class);
		config.setInputFile("./WordCount/TropicThunderQuote.txt");
		config.setOutputFile("WCOutput");
		File tropicThunder = new File("./WordCount/TropicThunderQuote.txt");
		File[] files = new File[1];
		files[0] = tropicThunder;
		MapReducer map_reducer = null;
		try {
			map_reducer = new MapReducer("WordcountClient",new Host("unix3.andrew.cmu.edu",8080)); //update to config file
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
