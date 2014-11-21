package WordCount;

import java.util.ArrayList;

import IOFormat.ReducerCollector;
import MapReduce.Reducer;

public class WordCountReduce implements Reducer{

	@Override
	public void reduce(String key, ArrayList<String> value,
			ReducerCollector reducerCollector) {
		reducerCollector.addOutput(key, value.size());
	}
	public void poop() {
		System.out.println("poop");
	}

}
