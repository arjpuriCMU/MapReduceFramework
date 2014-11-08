package DFS;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DFSFile implements Serializable {
	private File dfs_file;
	private Map<Integer, DFSBlock> blockId_block_map;
	
	public DFSFile(File f){
		dfs_file = f;
		blockId_block_map = new HashMap<Integer,DFSBlock>();
		
	}
	
	public File getFile(){
		return this.dfs_file;
	}
	
	public Map<Integer, DFSBlock> getBlockIDMap(){
		return this.blockId_block_map;
	}
	
}
