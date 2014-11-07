package DFS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;

public class DFSNameNode implements Remote{
	public ConcurrentHashMap<String, List<DFSFile>> nodeId_file_map;
	public ConcurrentHashMap<DFSFile, String> file_directory_map;
	public List<Integer> node_ids;
	public int port;
	public ServerSocket server_socket;
	public Registry datanode_registry;
	public String store_path;
	public Set<DFSFile> all_dfsFiles;
	private int block_size = ConfigSettings.block_size;
	private String init_directory = ConfigSettings.init_file_directory;
	public DFSNameNode(int port, List<Integer> data_node_ids){
		nodeId_file_map = new ConcurrentHashMap<String,List<DFSFile>>();
		file_directory_map = new ConcurrentHashMap<DFSFile, String>();
		node_ids = data_node_ids;
		
		try {
			datanode_registry = LocateRegistry.createRegistry(port);
			datanode_registry.bind("DFSNameNode", this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
		this.port = port;
	}
	
	public void start(){
		try {
			server_socket = new ServerSocket(port);
		} catch (IOException e) {
			System.out.println("NameNode Connection Failed");
		}
		Socket data_node_socket;
		DFSConnectionManager connection_manager = new DFSConnectionManager(this);
		Thread connection_thread = new Thread(connection_manager);
		connection_thread.start();
	}

	public ServerSocket getServerSocket() {
		return this.server_socket;
	}
	
}
