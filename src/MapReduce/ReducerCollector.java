package MapReduce;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class ReducerCollector {

	public SortedMap<Object,ArrayList<Object>> reducerCollector = new TreeMap<Object,ArrayList<Object>>();
	
	public void addOutput(Object key, Object value){
		if (reducerCollector.containsKey(key)){
			reducerCollector.get(key).add(value);
			
		}
		else{
			ArrayList<Object> list = new ArrayList<Object>();
			list.add(value);
			reducerCollector.put(key, list);
		}
	}
}
