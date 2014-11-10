package Util;

import java.io.File;
import java.io.IOException;

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
}
