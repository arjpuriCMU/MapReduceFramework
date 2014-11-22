package IOFormat;

import java.io.Serializable;
import java.security.Key;

public class KeyValuePair implements Serializable, Comparable{

	private Object key;
	private Object value;
	public KeyValuePair(Object key, Object val) {
		this.key = key;
		this.value = val;
	}
	
	public boolean equals(Object object){
		KeyValuePair kvPair = (KeyValuePair) object;
		return key.equals(kvPair.key) && value.equals(kvPair.value);
	}

    @Override
    public int compareTo(Object O)
    {
        /* Note: Assumes keyClass implements Comparable */
        KeyValuePair kvp = (KeyValuePair) O;

        Class keyClass = key.getClass();
        Comparable castedKey = (Comparable) keyClass.cast(key);
        return castedKey.compareTo(keyClass.cast(kvp.getKey()));
    }
	
	public int hashCode(){
		return (key.toString() + value.toString()).hashCode();
	}
	
	public Object getKey(){
		return this.key;
	}
	
	public void setKey(Object key){
		this.key = key;
	}
	
	public Object getValue(){
		return this.value;
	}
	
	public void setValue(Object value){
		this.value = value;
	}

    public String toString(){
        return key.toString() + "=>" + value.toString();
    }

}
