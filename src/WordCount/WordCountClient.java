package WordCount;

import MapReduce.MapReduceInterface;
import MapReduce.MapReducer;
import Util.Host;

import java.io.File;

/**
 * Created by karansharma on 11/15/14.
 */
public class WordCountClient {
    public static void main(String[] args) throws Exception
    {
        if(args.length < 2)
            throw new Exception("Not Enough Parameters. Need to provide hostname and port");

        //Assumes args[0] is hostname and args[1] is port of master
        Host master = new Host(args[0],Integer.parseInt(args[1]));
        MapReducer map_reducer = new MapReducer("WordCounter",master);

        File[] files = File[1];
        File[0] = null;
        map_reducer.runJob((MapReduceInterface) Class.forName("WordCount"),files);
    }
}
