package DFS;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Messages.Handshake;
import Util.Host;

public class DFSConnectionManager extends UnicastRemoteObject implements Runnable, ConnectionManagerInterface {
	private DFSNameNode master_name_node;
	private ConcurrentHashMap<String,Socket> nodeId_socket_map;
	private List<String> node_ids;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	public DFSConnectionManager(DFSNameNode dfsNameNode) throws RemoteException {
		master_name_node = dfsNameNode;
		this.nodeId_socket_map = new ConcurrentHashMap<String,Socket>();
		node_ids = new ArrayList<String>();
		initOnRegistry();
	}
	private void initOnRegistry() {
		try {
			Registry registry = LocateRegistry.getRegistry(master_name_node.getHost().getHostName(),REGISTRY_PORT);
			registry.bind(DFSConfig.CONNECTION_MANAGER_ID, this);	
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
					
	}
	private boolean active = true;
	
	public void setActive(boolean bool){
		this.active = false;
	}
	public void run(){
		System.out.println("Connection Manager started...");
		ServerSocket server_socket = master_name_node.getServerSocket();
		Socket data_node_socket = null;
		ObjectInputStream input_stream = null;
		int i = 0;
		master_name_node.startHealthChecker(node_ids);
		while(active){
			try {
				data_node_socket = server_socket.accept();
				input_stream = new ObjectInputStream(data_node_socket.getInputStream());
				Handshake handshake_msg = (Handshake) input_stream.readObject(); //assumes datanode will always send a message
				this.nodeId_socket_map.put(handshake_msg.getNodeId(), data_node_socket);
				if (master_name_node.getNodeIds().contains(handshake_msg.getNodeId())){
					Thread.sleep(1000);
					master_name_node.returnFilesToNode(handshake_msg.getNodeId());
				}
				else{
					master_name_node.getIdBlockMap().put(handshake_msg.getNodeId(), new ArrayList<DFSBlock>());
				}
				master_name_node.node_ids.add(handshake_msg.getNodeId());
				master_name_node.getIdHostMap().put(handshake_msg.getNodeId()
						,new Host(data_node_socket.getInetAddress().getHostName()
								,data_node_socket.getPort()));
				i++;
				Registry registry = LocateRegistry.getRegistry(master_name_node.getServerSocket().getInetAddress().getHostName()
						,REGISTRY_PORT);
				master_name_node.getIdActiveMap().put(handshake_msg.getNodeId(), true);
				
				HealthMonitor health_monitor = (HealthMonitor) registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
				health_monitor.addNode(handshake_msg.getNodeId());
				System.out.println("DataNode Id: " + handshake_msg.getNodeId() + " has started..");
				System.out.print("NameNode -> ");
				data_node_socket.close();
				input_stream.close();
			} catch (IOException e) {
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	
	}
	
}
