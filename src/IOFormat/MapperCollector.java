package IOFormat;

import java.util.ArrayList;

public class MapperCollector {
	
	public ArrayList<KeyValuePair> mapperOutputCollection = new ArrayList<KeyValuePair>();
	
	public void addOutput(Object key, Object val){
		KeyValuePair kvPair = new KeyValuePair(key,val);
		mapperOutputCollection.add(kvPair);
	}

}
