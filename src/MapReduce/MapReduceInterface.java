package MapReduce;

import java.io.File;

/**
 * Created by karansharma on 11/13/14.
 */
public interface MapReduceInterface {
    void Map(File inFile, File outFile);
    void Reduce(File inFile,File outFile);


}
