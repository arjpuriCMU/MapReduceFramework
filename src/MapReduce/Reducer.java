package MapReduce;

import java.util.ArrayList;

import IOFormat.ReducerCollector;

public interface Reducer {

	public void reduce(String key, ArrayList<String> value, ReducerCollector reducerCollector);
	public void poop();
}
