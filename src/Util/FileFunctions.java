package Util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }


}
