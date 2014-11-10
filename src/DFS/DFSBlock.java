package DFS;

import java.util.Set;

import Util.Host;
import Util.Tuple;

public class DFSBlock {
	private int block_no;
	private Set<Host> replica_hosts;
	private Tuple<Integer,Integer> block_range;
	public final String STORAGE_PATH = DFSConfig.DFS_STORAGE_PATH;
	private String file_name;
	
	
	public DFSBlock(String file_name, int block_no, Tuple<Integer,Integer> range, Set<Host> replica_hosts){
		this.block_no = block_no;
		this.block_range = range;
		this.replica_hosts = replica_hosts;
		this.file_name = file_name;
	}
	
	public String getLocalBlockPath(){
		return STORAGE_PATH + file_name + "_" + block_no;
	}
	
	public String getHostBlockPath(String dataNode_id){
		return STORAGE_PATH + dataNode_id +  "/" + file_name + "_" + block_no; 
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
	
	public Tuple<Integer,Integer> getBlockRange(){
		return this.block_range;
	}
	
}
