package DFS;

import java.io.Serializable;
import java.util.Set;

import Config.InternalConfig;
import Util.FileFunctions;
import Util.Host;
import Util.Tuple;

public class DFSBlock implements Serializable {
	
	private static final long serialVersionUID = 1042946964451470141L;
	private int block_no;
	private Set<Host> replica_hosts;
	private Tuple<Integer,Integer> block_range;
	public final String STORAGE_PATH = InternalConfig.DFS_STORAGE_PATH;
	private String file_name;
	private String file_id;
	
	public DFSBlock(String file_name, String file_id, int block_no, Tuple<Integer,Integer> range, Set<Host> replica_hosts){
		this.block_no = block_no;
		this.block_range = range;
		this.replica_hosts = replica_hosts;
		this.file_name = file_name;
		this.file_id = file_id;
	}
	public String getLocalBlockPath(){
		String file_name_no_ext = this.file_name.replaceFirst("[.][^.]+$", "");
		String ext = FileFunctions.getFileExtension(this.file_name);
		return STORAGE_PATH + file_name_no_ext + "_" + block_no + ext;
	}
	
	public String getHostBlockPath(String dataNode_id){
		String file_name_no_ext = this.file_name.replaceFirst("[.][^.]+$", "");
		String ext = FileFunctions.getFileExtension(this.file_name);
		return STORAGE_PATH + dataNode_id +  "/" + file_name_no_ext + "_" + block_no + ext; 
	}
	
	public void setBlockRange(Tuple<Integer,Integer> range){
		this.block_range = range;
	}
	
	public String getFileName(){
		return this.file_name;
	}
	
	
	
	public int getBlockNumber(){
		return this.block_no;
	}
	
	public Set<Host> getBlockHosts(){
		return this.replica_hosts;
	}
	
	public void setBlockHosts(Set<Host> s){
		this.replica_hosts = s;
	}
	
	public String toString(){
		return this.file_name + "," + this.block_no;
	}
	
	public Tuple<Integer,Integer> getBlockRange(){
		return this.block_range;
	}
	public String getCustomPath(int replica_no) {
		String file_name_no_ext = this.file_name.replaceFirst("[.][^.]+$", "");
		String ext = FileFunctions.getFileExtension(this.file_name);
		return STORAGE_PATH + file_name_no_ext + "_" + block_no +  "_" + replica_no+ ext;
	}
	@Override
	public int hashCode(){
		return this.file_name.hashCode() + this.block_no;
		
	}
	
}
