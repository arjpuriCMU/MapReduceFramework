package WordCount;

import MapReduce.MapReduceInterface;
import Util.Tuple;

/**
 * Created by karansharma on 11/15/14.
 */
public class WordCount implements MapReduceInterface {

    public String Map(String key){
        return "";
    }

    public Tuple<String,String> Reduce(Tuple<String,String> pair1, Tuple<String,String> pair2){
        return null;
    }

}
