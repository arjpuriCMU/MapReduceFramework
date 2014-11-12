package Util;

import java.io.Serializable;

public class Tuple<A,B> implements Serializable {
	private static final long serialVersionUID = 6152970163721457935L;
	private A first;
	private B second;
	
	public Tuple(A first, B second){
		this.first = first;
		this.second = second;
	}
	
	public Tuple() {
	}

	public A getFirst(){
		return this.first;
	}
	
	public B getSecond(){
		return this.second;
	}
	
	@Override
	public boolean equals(Object o){
		if (!(o instanceof Tuple<?,?>)){
			return false;
		}
		@SuppressWarnings("unchecked")
		Tuple<A,B> tup = (Tuple<A,B>) o;
		if (this.first.equals(tup.getFirst()) && this.second.equals(tup.getSecond())){
			return true;
		}
		return false;
	}
	
	public String toString(){
		return "|" + first + "," + second + "|";
	}
	
	public int hashCode(){
		return Math.abs(first.hashCode() + second.hashCode());
	}
}
