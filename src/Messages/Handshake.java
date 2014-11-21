package Messages;

public class Handshake implements Message {
	private String node_id;
	private String data_node_host;
	public Handshake(String dataNode_id, String data_node_host){
		this.node_id = dataNode_id;
		this.data_node_host = data_node_host;
	}
	
	public String getNodeId(){
		return this.node_id;
	}
	
	public String getDataNodeHost(){
		return this.data_node_host;
	}
	
	
}
