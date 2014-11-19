package IOFormat;

import java.io.Serializable;

public class KeyValuePair implements Serializable{

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
	
	
	

}
