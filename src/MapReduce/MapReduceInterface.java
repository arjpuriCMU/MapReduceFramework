package MapReduce;

import Util.Tuple;

import java.io.File;

/**
 * Created by karansharma on 11/13/14.
 */
public interface MapReduceInterface {
    String Map(String key);
    Tuple<String,String> Reduce(Tuple<String,String> pair1, Tuple<String,String> pair2);


}
