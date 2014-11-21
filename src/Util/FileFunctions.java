package Util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileFunctions {
	
	private static final int COPY_BUF_SIZE = 8024;


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
    
    public static byte[] convertClassToByteArray(File file) throws IOException{
    	 int _offset=0;
         int _read=0;

         InputStream fileInputStream = new FileInputStream(file);
         
         byte[] byteArray = new byte[(int)file.length()];
         while (_offset < byteArray.length && (_read=fileInputStream.read(byteArray, _offset, byteArray.length-_offset)) >= 0)
             _offset += _read;    

         fileInputStream.close();
         
         return byteArray;
    }
    
    public static long copy(final InputStream input, final OutputStream output) throws IOException {
        return copy(input, output, COPY_BUF_SIZE);
    }
    
    public static long copy(final InputStream input, final OutputStream output, int buffersize) throws IOException {
        final byte[] buffer = new byte[buffersize];
        int n = 0;
        long count=0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }
    
    public static byte[] toByteArray(final InputStream input) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        copy(input, output);
        return output.toByteArray();
    }

    /*
        TODO: Write function to be able to get key and value (stored as strings)
        for a the current line number. This will be utilized for the reduce
        function. For now wait until implementing the reducer on a slave node
        then come back to this
     */
    public static Tuple<String,String> returnKeyValue(File file){

        //BufferedReader br = new BufferedReader(new FileReader(file));
        return null;

    }
    
    public static int countLines(String filename) throws IOException {
    	BufferedReader reader = new BufferedReader(new FileReader(filename));
    	int lines = 0;
    	while (reader.readLine() != null) lines++;
    	reader.close();
		return lines;
    }


}
