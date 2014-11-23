package WordCount;

import java.util.ArrayList;

import IOFormat.MRCollector;
import MapReduce.Reducer;

public class WordCountReduce implements Reducer{

	@Override
	public void reduce(String key, ArrayList<String> value,
			MRCollector reducerCollector) {
		reducerCollector.addOutput(key, value.size());
	}
}
