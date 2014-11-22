package IOFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class MapperCollector {
	
	private ArrayList<KeyValuePair> mapperOutputCollection = new ArrayList<KeyValuePair>();
    //private SortedMap<Object,Object> mapperOutputCollection = new TreeMap<>();

	public void addOutput(Object key, Object val){
		mapperOutputCollection.add(new KeyValuePair(key, val));
	}

    public ArrayList<KeyValuePair> getMapperOutputCollection()
    {
        Collections.sort(mapperOutputCollection);
        return  mapperOutputCollection;
    }

}
