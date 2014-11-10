package DFS;

import java.io.File;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import Messages.Handshake;
import Util.FileFunctions;
import Util.Tuple;

public class DFSDataNode implements Remote  {

	public InetAddress name_node_host;
	public int port;
	public boolean active;
	public final String STORAGE_PATH = DFSConfig.DFS_STORAGE_PATH;
	public ServerSocket server_socket;
	public String data_nodeId;
	public HashMap<String, List<DFSBlock>> file_block_replicas_map; 
	public HashMap<Tuple<String,Integer>,File> block_file_map;
	private String store_path;
	private int health = 100;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	
	public DFSDataNode(String data_nodeId, InetAddress inetAddress,int port){
		this.active = true;
		this.name_node_host = inetAddress;
		this.port = port;
		this.data_nodeId = data_nodeId;
		this.active = true;
		this.store_path = STORAGE_PATH + data_nodeId + "/";
		file_block_replicas_map = new HashMap<String,List<DFSBlock>>();
		block_file_map = new HashMap<Tuple<String,Integer>,File>();
	}
	
	public void setActive(boolean bool){
		this.active = bool;
	}
	
	public String getHeartbeatHelperID(){
		return this.data_nodeId+ "heartbeat";
	}
	
	@SuppressWarnings("resource")
	public void start(){
		System.out.println("DFSDataNode ID " + this.data_nodeId + " is starting...");
		try {
			Socket socket = new Socket(name_node_host.getHostAddress(),port);
			ObjectOutputStream output_stream = new ObjectOutputStream(socket.getOutputStream());
			output_stream.writeObject(new Handshake(this.data_nodeId));
			
		} catch (IOException e) {
			System.out.println("DFS Node unable to connect");
		}
		System.out.println("DFSDataNode ID " + this.data_nodeId + " binding with NameNode Registry");
		Remote current_stub;
		try {
			current_stub =  UnicastRemoteObject.exportObject(this,REGISTRY_PORT);
			Registry registry = LocateRegistry.getRegistry(this.name_node_host.getHostAddress(), REGISTRY_PORT);
			registry.bind(this.data_nodeId, current_stub);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
		FileFunctions.createDirectory(store_path);
		startHeartbeat();
		
	}

	private void startHeartbeat() {
		System.out.println("Heartbeat Started");
		DataNodeHeartbeatHelper heartbeat_helper = new DataNodeHeartbeatHelper(this.data_nodeId, name_node_host.getHostName(), port);
		Thread th = new Thread(heartbeat_helper);
		th.start();
	}
	
	@SuppressWarnings("resource")
	public void sendHeartbeatMessage(String host, int port){
		try {
			Socket socket = new Socket(host,port);
			ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
			output.writeObject(new Handshake(this.data_nodeId));
			output.flush();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void initiateBlock(File block_file, DFSBlock file_block) {
		this.block_file_map.put(new Tuple<String,Integer>(file_block.getFileName(),file_block.getBlockNumber()),block_file);
		if (!this.file_block_replicas_map.containsKey(file_block.getFileName())){
			this.file_block_replicas_map.put(file_block.getFileName(), new ArrayList<DFSBlock>());
		}
		else{
			this.file_block_replicas_map.get(file_block.getFileName()).add(file_block);
		}
		block_file.renameTo(new File(file_block.getHostBlockPath(this.data_nodeId)));
		try {
			FileFunctions.createFile(block_file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		
	}
}
