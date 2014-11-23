package IOFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class MRCollector {
	
	private ArrayList<KeyValuePair> OutputCollection = new ArrayList<KeyValuePair>();

	public void addOutput(Object key, Object val){
		OutputCollection.add(new KeyValuePair(key, val));
	}

    public ArrayList<KeyValuePair> getOutputCollection()
    {
        return  OutputCollection;
    }

}
