package Util;

public class JavaCustomClassLoader extends ClassLoader {
	private byte[] data;
	
	public JavaCustomClassLoader(byte[] data){
		super();
		this.data = data;
	}
	
	public void setByteArray(byte[] data){
		this.data = data;
	}
	
	public Class<?> findClass(String name){
		Class<?> c = defineClass(name,data,0,data.length);
		this.resolveClass(c);
		return c;
	}


}
