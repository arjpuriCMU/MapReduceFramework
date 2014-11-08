package DFS;

import java.util.List;

import Util.Tuple;

public class DFSBlock {
	private int block_no;
	private List<String> block_hosts;
	private byte[] block_buffer;
	private Tuple<Integer,Integer> block_range;
	
	public DFSBlock(byte[] block, int block_no){
		this.block_buffer = block;
		this.block_no = block_no;
	}
	
}
