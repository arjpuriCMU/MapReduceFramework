package MapReduce;

import IOFormat.MRCollector;

public interface Mapper {

	       public void map(String line, MRCollector mapperOutputCollector);

}
