package Util;

public class JavaCustomClassLoader extends ClassLoader {
	private byte[] data;
	
	public void setByteArray(byte[] data){
		this.data = data;
	}
	
	public static void invokeMethod(String class_name, String method_name){
        JavaCustomClassLoader _classLoader = new JavaCustomClassLoader();
        byte[] rawBytes = new byte[_classLoader.data.length];
        for (int index = 0; index < rawBytes.length; index++)
            rawBytes[index] = (byte) _classLoader.data[index];
        Class regeneratedClass = _classLoader.defineClass(class_name, rawBytes, 0, rawBytes.length);        
        regeneratedClass.getMethod(method_name, null).invoke(null, new Object[] { args });
        }		
	}

}
