package DFS;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/*Wrapper of the input file that allows unique identification of file */
public class DFSFile implements Serializable {
	private static final long serialVersionUID = 1L;
	private File dfs_file;
	private Map<Integer, DFSBlock> blockId_block_map;
	private final String  DFSFile_id;
	public DFSFile(File f, String DFSFile_id){
		dfs_file = f;
		blockId_block_map = new HashMap<Integer,DFSBlock>();
		this.DFSFile_id = DFSFile_id + f.getName();
		
	}
	public File getFile(){
		return this.dfs_file;
	}
	
	public Map<Integer, DFSBlock> getBlockIDMap(){
		return this.blockId_block_map;
	}

	public String getDFSFile_id() {
		return DFSFile_id;
	}
	
}
