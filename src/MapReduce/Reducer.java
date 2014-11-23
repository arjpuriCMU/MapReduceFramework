package MapReduce;

import java.util.ArrayList;

import IOFormat.MRCollector;

public interface Reducer {

	public void reduce(String key, ArrayList<String> value, MRCollector reducerCollector);
}
