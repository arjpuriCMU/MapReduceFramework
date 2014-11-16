package DFS;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
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

public class DFSDataNode extends UnicastRemoteObject implements DataNodeInterface {

	public InetAddress name_node_host;
	public int port;
	public boolean active;
	public final String STORAGE_PATH = DFSConfig.DFS_STORAGE_PATH;
	public String data_nodeId;
	public HashMap<String, List<DFSBlock>> file_block_replicas_map; 
	public HashMap<Tuple<String,Integer>,File> block_file_map;
	private String store_path;
	private int health = 100;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	
	public DFSDataNode(String data_nodeId, InetAddress inetAddress,int port) throws RemoteException{
		this.active = true;
		this.name_node_host = inetAddress;
		this.port = port;
		this.data_nodeId = data_nodeId;
		this.store_path = STORAGE_PATH + data_nodeId + "/";
		file_block_replicas_map = new HashMap<String,List<DFSBlock>>();
		block_file_map = new HashMap<Tuple<String,Integer>,File>();
	}
	
	public void setActive(boolean bool){
		this.active = bool;
	}

	public String getNodeId(){
		return this.data_nodeId;
	}
	
	public void exitDataNode(){
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(this.name_node_host.getHostAddress(), REGISTRY_PORT);
			registry.unbind(this.data_nodeId);
			registry.unbind(this.getHeartbeatHelperID());
		} catch (RemoteException e) {
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		System.exit(0);
		System.out.println("Exiting DataNode");
	}
	
	public String getHeartbeatHelperID(){
		return this.data_nodeId+ "heartbeat";
	}
	
	public HashMap<Tuple<String,Integer>,File> getBlockFileMap(){
		return this.block_file_map;
	}
	
	public HashMap<String,List<DFSBlock>> getFileBlockReplicaMap(){
		return this.file_block_replicas_map;
	}
	
	@SuppressWarnings("resource")
	public void start(){
		System.out.println("DFSDataNode ID " + this.data_nodeId + " is starting...");
		try {
			Socket socket = new Socket(name_node_host.getHostAddress(),port);
			ObjectOutputStream output_stream = new ObjectOutputStream(socket.getOutputStream());
			output_stream.writeObject(new Handshake(this.data_nodeId));
			socket.close();
			
		} catch (IOException e) {
			System.out.println("DFS Node unable to connect");
		}
		System.out.println("DFSDataNode ID " + this.data_nodeId + " binding with NameNode Registry");
		try {
			Registry registry = LocateRegistry.getRegistry(this.name_node_host.getHostAddress(), REGISTRY_PORT);
			registry.rebind(this.data_nodeId, this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} 
		FileFunctions.createDirectory(store_path);
		startHeartbeat();
		
	}

	private void startHeartbeat() {
		DataNodeHeartbeatHelper heartbeat_helper = null;
		try {
			heartbeat_helper = new DataNodeHeartbeatHelper(this.data_nodeId, name_node_host.getHostName(), port);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		Thread th = new Thread(heartbeat_helper);
		th.start();
		System.out.println("Heartbeat Started");
	}
	

	public void initiateBlock(byte[] byte_array, DFSBlock file_block) {
		System.out.println("DataNode Id: " + this.data_nodeId + " recieved file block " + file_block.getLocalBlockPath());
		File block_file = new File(file_block.getHostBlockPath(this.data_nodeId));
		int bytesRead;
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(block_file);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			bos.write(byte_array, 0, byte_array.length);
			bos.flush();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	
	
}
