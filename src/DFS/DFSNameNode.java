package DFS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Util.Tuple;
import Config.ConfigSettings;

public class DFSNameNode implements Remote{
	public ConcurrentHashMap<String, List<DFSFile>> nodeId_file_map;
	public ConcurrentHashMap<DFSFile, String> file_directory_map;
	public HashMap<Tuple<DFSFile,DFSBlock>, String> file_block_host_map;
	
	public List<String> node_ids;
	
	public int port;
	public ServerSocket server_socket;
	
	public Registry datanode_registry;
	public String store_path;
	public List<DFSFile> all_dfsFiles;
	
	private int BLOCK_SIZE = ConfigSettings.block_size;
	private String INIT_DIRECTORY = ConfigSettings.init_file_directory;
	
	
	
	public DFSNameNode(int port){
		node_ids = new ArrayList<String>();
		nodeId_file_map = new ConcurrentHashMap<String,List<DFSFile>>();
		file_directory_map = new ConcurrentHashMap<DFSFile, String>();
		file_block_host_map = new HashMap<Tuple<DFSFile,DFSBlock>, String>();
		try {
			datanode_registry = LocateRegistry.createRegistry(port);
			datanode_registry.bind("DFSNameNode", this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
		this.port = port;
		initDFSFiles();
	}
	
	private void initDFSFiles() {
		File dir = new File(INIT_DIRECTORY);
		File[] listing = dir.listFiles();
		if (listing != null){
			for (File job_file : listing){
				all_dfsFiles.add(new DFSFile(job_file));
			}
		}
		else{
			//Could not find that directory
			System.out.println("init directory does not exist");
		}
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
	
	public void partitionAndDistributeFiles(){
		int total = 0;
		for (int i = 0; i < this.all_dfsFiles.size(); i++){
			String line;
			int block_no = 0;
			DFSFile file = this.all_dfsFiles.get(i);
//			FileInputStream input_stream;
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(file.getFile()));
//				input_stream = new FileInputStream(file.getFile());
				byte[] buffer = new byte[BLOCK_SIZE];
				while ((line = br.readLine()) != null){
					file.getBlockIDMap().put(block_no, new DFSBlock(buffer,block_no));
					block_no++;
					total++;
				}
//				input_stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		int distribute_size = total/this.node_ids.size();
		
		for (int i = 0; i < distribute_size; i++){
			
		}
		
	}

	public ServerSocket getServerSocket() {
		return this.server_socket;
	}
	
}
