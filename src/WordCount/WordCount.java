package WordCount;

import java.io.File;
import java.io.IOException;

import Config.InternalConfig;
import MapReduce.MapReducerClient;
import MapReduce.MapReducerConfig;
import Util.FileFunctions;
import Util.Host;
import Util.Tuple;

public class WordCount {

	public static void main(String[] args) throws IOException{
		MapReducerConfig config = new MapReducerConfig();
		config.setMapperClass(WordCountMap.class);
		config.setReducerClass(WordCountReduce.class);
		config.setInputFile("./src/WordCount/TropicThunderQuote.txt");
		config.setOutputFilePath(InternalConfig.DFS_STORAGE_PATH);
		File tropicThunder = new File("./src/WordCount/TropicThunderQuote.txt");
		File[] files = new File[1];
		files[0] = tropicThunder;
		MapReducerClient map_reducer = null;
		try {
			map_reducer = new MapReducerClient("WordcountClient",new Host("unix3.andrew.cmu.edu",8080)); //update to config file
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		try {
			map_reducer.runJob(config, files);
			map_reducer.startInterface();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
