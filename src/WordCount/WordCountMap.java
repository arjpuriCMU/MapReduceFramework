package WordCount;

import IOFormat.MRCollector;
import MapReduce.Mapper;

public class WordCountMap implements Mapper {

	@Override
	public void map(String line, MRCollector mapperOutputCollector) {
		String[] words = line.split(" ");
        for(String word : words) {
            mapperOutputCollector.addOutput(word, 1);
        }
	}
	
	public void sex(){
		System.out.println("sexysexy");
	}

}
