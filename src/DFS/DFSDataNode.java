package DFS;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import Messages.Handshake;

public class DFSDataNode implements Remote  {

	public InetAddress name_node_host;
	public int port;
	public boolean active;
	
	public ServerSocket server_socket;
	public String data_nodeId;
	public List<DFSFile> file_block_replicas; //File based system (NOT BLOCKS)
	private String store_path;
	public DFSDataNode(String data_nodeId, InetAddress inetAddress,int port){
		this.active = true;
		this.name_node_host = inetAddress;
		this.port = port;
		this.data_nodeId = data_nodeId;
		this.store_path = "./dfs_storage/" + data_nodeId + "/";
	
	}
	
	public void start(){
		try {
			Socket socket = new Socket(name_node_host.getHostAddress(),port);
			ObjectOutputStream output_stream = new ObjectOutputStream(socket.getOutputStream());
			output_stream.writeObject(new Handshake(this.data_nodeId));
			
		} catch (IOException e) {
			System.out.println("DFS Node unable to connect");
		}
		System.out.println("DFSDataNode ID " + this.data_nodeId + " binding with NameNode Registry");
		DFSDataNode current_stub;
		try {
			current_stub = (DFSDataNode) UnicastRemoteObject.exportObject(this,port);
			Registry registry = LocateRegistry.getRegistry(this.name_node_host.getHostAddress(), port);
			registry.bind(this.data_nodeId, current_stub);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public static void main(String[] args){
		
	}
}
