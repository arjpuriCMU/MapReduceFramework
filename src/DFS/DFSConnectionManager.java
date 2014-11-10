package DFS;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Messages.Handshake;
import Util.Host;

public class DFSConnectionManager implements Runnable {
	private DFSNameNode master_name_node;
	private ConcurrentHashMap<String,Socket> nodeId_socket_map;
	private List<String> node_ids;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	public DFSConnectionManager(DFSNameNode dfsNameNode) {
		master_name_node = dfsNameNode;
		this.nodeId_socket_map = new ConcurrentHashMap<String,Socket>();
		node_ids = new ArrayList<String>();
	}
	
	public void run(){
		System.out.println("Connection Manager started...");
		ServerSocket server_socket = master_name_node.getServerSocket();
		Socket data_node_socket = null;
		ObjectInputStream input_stream = null;
		int i = 0;
		master_name_node.startHealthChecker(node_ids);
		while(true){
			try {
				data_node_socket = server_socket.accept();
				input_stream = new ObjectInputStream(data_node_socket.getInputStream());
				Handshake handshake_msg = (Handshake) input_stream.readObject(); //assumes datanode will always send a message
				this.nodeId_socket_map.put(handshake_msg.getNodeId(), data_node_socket);
				master_name_node.node_ids.add(handshake_msg.getNodeId());
				master_name_node.getIdHostMap().put(handshake_msg.getNodeId()
						,new Host(data_node_socket.getInetAddress().getHostName()
								,data_node_socket.getPort()));
				i++;
				Registry registry = LocateRegistry.getRegistry(master_name_node.getServerSocket().getInetAddress().getHostName()
						,REGISTRY_PORT);
				DFSHealthMonitor health_monitor = (DFSHealthMonitor) registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
				health_monitor.addNode(handshake_msg.getNodeId());
				System.out.println("DataNode Id: " + handshake_msg.getNodeId() + " has started..");
			} catch (IOException e) {
				System.out.println("Could not form connection with DataNode");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} 
		}
		
	
	}
}
