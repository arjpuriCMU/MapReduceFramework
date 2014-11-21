package WordCount;

import IOFormat.MapperCollector;
import MapReduce.Mapper;

public class WordCountMap implements Mapper {

	@Override
	public void map(Object key, Object value,
			MapperCollector mapperOutputCollector) {
		String temp_key = (String) key;
		int temp_value = 1;
		mapperOutputCollector.addOutput(temp_key, temp_value);
	}
	
	public void sex(){
		System.out.println("sexysexy");
	}

}
