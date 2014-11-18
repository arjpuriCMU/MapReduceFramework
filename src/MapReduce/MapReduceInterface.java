package MapReduce;

import Util.Tuple;

/**
 * Created by karansharma on 11/13/14.
 */
public interface MapReduceInterface {
    String Map(String record);
    Tuple<String,String> Reduce(Tuple<String,String> pair1, Tuple<String,String> pair2);


}
