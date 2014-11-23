package SumOfSquares;

import java.io.File;
import java.io.IOException;

import Config.InternalConfig;
import MapReduce.MapReducerClient;
import MapReduce.MapReducerConfig;
import Util.Host;

public class SumOfSquares {
	public static void main(String[] args) throws IOException{
		MapReducerConfig config = new MapReducerConfig();
		config.setMapperClass(SumOfSquaresMap.class);
		config.setReducerClass(SumOfSquaresReduce.class);
		config.setOutputFilePath(InternalConfig.DFS_STORAGE_PATH);
		File squares1 = new File("./src/SumOfSquares/squares1.txt");
		File squares2 = new File("./src/SumOfSquares/squares2.txt");
		File[] files = new File[2];
		files[0] = squares1;
		files[1] = squares2;
		MapReducerClient map_reducer = null;
		try {
			map_reducer = new MapReducerClient("SumOfSquaresClient",new Host("Arjuns-MacBook-Pro-2.local",8080)); //update to config file
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
