package Messages;

public class Handshake implements Message {
	private String node_id;
	public Handshake(String dataNode_id){
		this.node_id = dataNode_id;
	}
	
	public String getNodeId(){
		return this.node_id;
	}
	
	
}
