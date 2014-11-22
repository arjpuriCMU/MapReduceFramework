package IOFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class ReducerCollector {

    private ArrayList<KeyValuePair> reducerOutputCollection = new ArrayList<KeyValuePair>();

    public void addOutput(Object key, Object val){
        reducerOutputCollection.add(new KeyValuePair(key, val));
    }

    public ArrayList<KeyValuePair> getReducerOutputCollection()
    {
        /* Already Sorted */
        return  reducerOutputCollection;
    }
}
