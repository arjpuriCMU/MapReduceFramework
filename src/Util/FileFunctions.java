package Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileFunctions {

	public static void createDirectory(String direc){
		File dir = new File(direc);
		if (!dir.exists()){
			dir.mkdirs();
		}
		return;
	}
	
	public static void createFile(File path) throws IOException{
		if (path.exists() == false){
			File par = path.getParentFile();
			
			if (par.exists() == false){
				par.mkdirs();
			}
			path.createNewFile();
		}
		else{
			path.delete();
			path.createNewFile();
		}
	}
	
	 public static String getFileExtension(String fileName) {
	        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
	        return fileName.substring(fileName.lastIndexOf("."));
	        else return "";
	    }
	 
	 public static void deleteDirectory(File file) {
	        if (!file.exists())
	            return;
	         
	        if (file.isDirectory()) {
	            for (File f : file.listFiles()) {
	                deleteDirectory(f);
	            }
	        }
	        file.delete();
	 }
	
    
    public static void copy(File source, File target) throws IOException {
        InputStream in = new FileInputStream(source);
        OutputStream out = new FileOutputStream(target);
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
    }
}
