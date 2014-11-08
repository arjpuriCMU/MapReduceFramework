package DFS;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import Messages.Handshake;

public class DFSConnectionManager implements Runnable {
	DFSNameNode master_name_node;
	ConcurrentHashMap<String,Socket> nodeId_socket_map;

	public DFSConnectionManager(DFSNameNode dfsNameNode) {
		master_name_node = dfsNameNode;
		this.nodeId_socket_map = new ConcurrentHashMap<String,Socket>();
	}
	
	public void run(){
		ServerSocket server_socket = master_name_node.getServerSocket();
		Socket data_node_socket = null;
		while(true){
			try {
				data_node_socket = server_socket.accept();
			} catch (IOException e) {
				System.out.println("Could not form connection with DataNode");
			} 
			ObjectInputStream input_stream = null;
			try {
				input_stream = new ObjectInputStream(data_node_socket.getInputStream());
				Handshake handshake_msg = (Handshake) input_stream.readObject(); //assumes datanode will always send a message
				this.nodeId_socket_map.put(handshake_msg.getNodeId(), data_node_socket);
				master_name_node.node_ids.add(handshake_msg.getNodeId());
			} catch (IOException e) {
				e.printStackTrace();
		
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	
	}
}
