package DFS;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Util.FileFunctions;
import Util.Host;
import Util.Tuple;
import Config.ConfigSettings;

public class DFSNameNode extends UnicastRemoteObject implements DFSNameNodeInterface{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6201621654572231214L;
	public ConcurrentHashMap<String, List<DFSFile>> nodeId_file_map;
	public ConcurrentHashMap<DFSFile, String> file_directory_map;
	public HashMap<Tuple<DFSFile,DFSBlock>, String> file_block_host_map;
	public ConcurrentHashMap<String,Host> nodeId_host_map;
	public ConcurrentHashMap<String,Boolean> nodeId_active_map;
	public ConcurrentHashMap<String,List<DFSBlock>> nodeId_block_map;
	public Set<String> node_ids;
	public int port;
	public ServerSocket server_socket;
	public Registry main_registry;
	public String store_path;
	public List<DFSFile> all_dfsFiles;
	
	private String INIT_DIRECTORY = ConfigSettings.init_file_directory;
	private final int SPLIT_SIZE = ConfigSettings.split_size;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	public HashMap<Tuple<String,Integer>,File> block_file_map;
	
	/*TO DO*/
	//MAKE SURE REPLICAS ARE ALSO TRACKED. RIGHT NOW HASHTABLE FORGETS REPLICAS?
	//Gracefully close all datanodes and heartbeat helpers on quit
	//If node restarts before death then reallocate all files
	
	
	public DFSNameNode(int port) throws RemoteException{
		node_ids = new HashSet<String>();
		nodeId_file_map = new ConcurrentHashMap<String,List<DFSFile>>();
		file_directory_map = new ConcurrentHashMap<DFSFile, String>();
		file_block_host_map = new HashMap<Tuple<DFSFile,DFSBlock>, String>();
		nodeId_host_map  = new ConcurrentHashMap<String,Host>();
		nodeId_active_map = new ConcurrentHashMap<String,Boolean>();
		all_dfsFiles = new ArrayList<DFSFile>();
		nodeId_block_map = new ConcurrentHashMap<String,List<DFSBlock>>();
		block_file_map = new HashMap<Tuple<String,Integer>,File>();
		
		try {
			main_registry = LocateRegistry.createRegistry(REGISTRY_PORT);
			main_registry.bind(DFSConfig.NAME_NODE_ID, this);
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
	
	public Set<String> getNodeIds(){
		return this.node_ids;
	}
	
	
	
	public void moveBlocksFromInactive(){
		System.out.println("Moving files from inactive dataNodes");
		List<DFSBlock> unassociated_replica_blocks = new ArrayList<DFSBlock>();
		List<String> active_nodes = getActiveNodes();
		List<String> inactive_nodes = new ArrayList<String>();
		for (String node_id : this.nodeId_active_map.keySet()){
			if (nodeId_active_map.get(node_id) == false){
				unassociated_replica_blocks.addAll(nodeId_block_map.get(node_id));
				inactive_nodes.add(node_id);
			}	
		}
		LinkedList<String> queue = new LinkedList<String>();
		for (String node_id: active_nodes){
			queue.addLast(node_id);
		}
		String destination_node;
		DFSFile dfs_file;
		File block_file;
		for (DFSBlock block : unassociated_replica_blocks){
			destination_node = queue.removeFirst();
			queue.addLast(destination_node);
			dfs_file = find_file(block.getFileName());
			Tuple<String,Integer> tuple = new Tuple<String,Integer>(dfs_file.getFile().getName(),block.getBlockNumber());
			block_file = block_file_map.get(tuple);
			byte[] byte_array = new byte[(int) block_file.length()];
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(block_file);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
			BufferedInputStream bis = new BufferedInputStream(fis);
			try {
				bis.read(byte_array,0,byte_array.length);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(dfs_file,block),destination_node);
			block.getBlockHosts().add(this.nodeId_host_map.get(destination_node));
			try {
				DataNodeInterface current_data_node = (DataNodeInterface) this.main_registry.lookup(destination_node);
				current_data_node.initiateBlock(byte_array, block);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
			
			dfs_file.getBlockIDMap().put(block.getBlockNumber(), block);
			nodeId_block_map.get(destination_node).add(block);
		}
		
		for (String node_id : inactive_nodes){
			node_ids.remove(node_id);
			nodeId_file_map.remove(node_id);
			RemoveNodesFromHostMap(node_id);
			nodeId_host_map.remove(node_id);
			nodeId_active_map.remove(node_id);
			nodeId_block_map.remove(node_id);
			
		}
	}
	
	private void RemoveNodesFromHostMap(String node_id) {
		for (Tuple<DFSFile, DFSBlock> tuple: this.file_block_host_map.keySet()){
			if (file_block_host_map.get(tuple).equals(node_id)){
				file_block_host_map.remove(tuple);
			}
		}
	}


	private void printBlockFiles(){
		for (Tuple<String, Integer> f : block_file_map.keySet()){
			System.out.print(f.getFirst() + ",, ");
			System.out.print(f.getSecond()+ ", ");
			System.out.println(block_file_map.get(f).getAbsolutePath());
			
		}
	}
	private DFSFile find_file(String fileName) {
		for (DFSFile file : this.all_dfsFiles){
			if (fileName.equals(file.getFile().getName())){
				return file;
			}
		}
		return null;
	
	}

	public List<String> getActiveNodes(){
		List<String> active_nodes = new ArrayList<String>();
		for (String node_id: this.nodeId_active_map.keySet()){
			if (nodeId_active_map.get(node_id) == true){
				active_nodes.add(node_id);
			}
		}
		return active_nodes;
	}
	
	public void changeActiveStatus(String node_id){
		this.nodeId_active_map.put(node_id, false);
	}
	public ConcurrentHashMap<String,Boolean> getIdActiveMap(){
		return this.nodeId_active_map;
	}
	public ConcurrentHashMap<String, Host> getIdHostMap(){
		return this.nodeId_host_map;
	}
	
	public ConcurrentHashMap<String, List<DFSBlock>> getIdBlockMap(){
		return this.nodeId_block_map;
	}

	@SuppressWarnings("resource")
	public void start(){
		System.out.println("Starting NameNode...");
		try {
			server_socket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		DFSConnectionManager connection_manager = null;
		try {
			connection_manager = new DFSConnectionManager(this);
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		Thread connection_thread = new Thread(connection_manager);
		connection_thread.start();
		Scanner scanner = new Scanner(System.in);
		String usrInput;
		String[] args;
		while(true){
			System.out.print("NameNode -> ");
			usrInput = scanner.nextLine();
			args = usrInput.split(" ");
			if (args[0].toLowerCase().equals("quit")){
				try {
					ConnectionManagerInterface c_manager = (ConnectionManagerInterface) this.main_registry.lookup(DFSConfig.CONNECTION_MANAGER_ID);
					c_manager.setActive(false);
				} catch (RemoteException | NotBoundException e1) {
					e1.printStackTrace();
				}
				
				closeDataNodes();
				FileFunctions.deleteDirectory(new File(DFSConfig.DFS_STORAGE_PATH));
				try {
					this.server_socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.exit(0);
			}
			processCLInput(args);

			if (args[0].toLowerCase().equals("help" )){
				displayHelp();
			}
		}
	}
	
	
	
	private void closeDataNodes() {
		for (String node_id: this.node_ids){
			try {
				DataNodeInterface data_node = (DataNodeInterface) this.main_registry.lookup(node_id);
				data_node.exitDataNode();
			} 
			catch (UnmarshalException e){
			}
			catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			} 
			
		}
	}

	private void displayHelp() {
		
	}

	private void processCLInput(String[] args) {
		if (args[0].toLowerCase().equals("data_nodes?")){ //Display all running workers
			System.out.println("DataNodes Present:");
			System.out.println("----------------");
			for (String node_id : node_ids){
				if (this.nodeId_active_map.get(node_id)){
					System.out.print("ACTIVE: ");
					System.out.println(node_id + " " + this.nodeId_host_map.get(node_id).hostname);
				}
				else{
					System.out.print("INACTIVE: ");
					System.out.println(node_id + this.nodeId_host_map.get(node_id).hostname);
				}
					
			}
			System.out.println("----------------");
		}
		else if (args[0].toLowerCase().equals("files?")){
			System.out.println("Files Present:");
			System.out.println("----------------");
			for (DFSFile dfs_file: this.all_dfsFiles){
				System.out.println(dfs_file.getFile().getName());
			}
			System.out.println("----------------");
		}
		
		else if (args[0].toLowerCase().equals("file_blocks?")){
			System.out.println("File Blocks Present:");
			System.out.println("----------------");
			for (Tuple<DFSFile,DFSBlock> tuple: this.file_block_host_map.keySet()){
				System.out.println(tuple.getFirst().getFile().getName() + " : " + tuple.getSecond().getBlockNumber());
			}
			System.out.println("----------------");
		}
		
		else if (args[0].toLowerCase().equals("distribute_files") || args[0].toLowerCase().equals("df") ){
			this.partitionAndDistributeFiles();
		}
		else if (args[0].toLowerCase().equals("datanode_health?")){
			System.out.println("----------------");
			try {
				HealthMonitor health_monitor = (HealthMonitor) main_registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
				health_monitor.printAllHealth();
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
			
			System.out.println("----------------");

		}
		else if (args[0].toLowerCase().equals("host?")){
			System.out.println("----------------");
			try {
				System.out.println(InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			System.out.println("----------------");

		}
		
		
	}

	public void partitionAndDistributeFiles(){
		LinkedList<String> data_node_queue = new LinkedList<String>();
		String line;
		BufferedReader br;
		BufferedWriter block_writer;
		int replication_factor = ConfigSettings.replication_factor;
		for (String node_id : this.node_ids){
			data_node_queue.addLast(node_id);
		}
		//Loop through all the files that need to be distributed
		for (int i = 0; i < this.all_dfsFiles.size(); i++){
			DFSFile file = this.all_dfsFiles.get(i);
			int line_no = 0;
			int block_no = 0;
			int previous_block_end = 0;
			DFSBlock file_block = new DFSBlock(file.getFile().getName(), block_no, new Tuple<Integer,Integer>(), null);
			Set<Host> block_hosts = new HashSet<Host>();
			List<String> to_send_nodes = new ArrayList<String>();
			boolean isEmpty = true;
			try {
				
				File block_file = new File(file_block.getLocalBlockPath()); //Create a temporary file to write lines to
				br = new BufferedReader(new FileReader(file.getFile()));
				block_writer = new BufferedWriter(new FileWriter(block_file));
				while ((line = br.readLine()) != null){ //loop through all lines
					if (line_no % SPLIT_SIZE == 0 && line_no != 0){  //when we need to split
						block_writer.close();
						block_hosts = new HashSet<Host>();
						file.getBlockIDMap().put(block_no, file_block);
						byte[] byte_array = new byte[(int) block_file.length()];
						FileInputStream fis = new FileInputStream(block_file);
						BufferedInputStream bis = new BufferedInputStream(fis);
						bis.read(byte_array,0,byte_array.length);
						for (int j = 0; j < replication_factor; j++){//to distribute replicas
							String node_id = data_node_queue.removeFirst();//next datablock in queue. this ensures replicas are on different nodes
							Host host = this.nodeId_host_map.get(node_id);							
							block_hosts.add(host);
							data_node_queue.addLast(node_id);
							to_send_nodes.add(node_id);
							file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(file,file_block),node_id);
							file_block.setBlockHosts(block_hosts);
							file_block.setBlockRange(new Tuple<Integer,Integer>(previous_block_end,line_no-1));
							file.getBlockIDMap().put(block_no, file_block);
							nodeId_block_map.get(node_id).add(file_block);
							this.block_file_map.put(new Tuple<String,Integer>(file_block.getFileName(),file_block.getBlockNumber()),block_file);
						}
						int replica_no = 1;
						//Here we actually send the replicas to the corresponding data_nodes using java RMI
						for (String nodeID : to_send_nodes){	
//							File copy = new File(file_block.getCustomPath(replica_no));
//							FileFunctions.copy(block_file, copy);
							DataNodeInterface current_data_node = (DataNodeInterface) this.main_registry.lookup(nodeID);
							current_data_node.initiateBlock(byte_array,file_block);
							replica_no++;
//							Files.delete(copy.toPath()); //delete the local replica
						}
						Files.delete(block_file.toPath()); //delete the local block_file
						to_send_nodes = new ArrayList<String>();
						isEmpty = true;
						previous_block_end = line_no;
						block_no++;
						file_block = new DFSBlock(file.getFile().getName(),block_no,new Tuple<Integer,Integer>(),null);
						block_file = new File(file_block.getLocalBlockPath());
						block_writer = new BufferedWriter(new FileWriter(block_file));
					}
					block_writer.write(line + "\n");
					line_no ++;
					isEmpty = false;
				}
				
				if (isEmpty == false){
					block_writer.close();
					br.close();
					block_hosts = new HashSet<Host>();
					file.getBlockIDMap().put(block_no, file_block);
					byte[] byte_array = new byte[(int) block_file.length()];
					FileInputStream fis = new FileInputStream(block_file);
					BufferedInputStream bis = new BufferedInputStream(fis);
					bis.read(byte_array,0,byte_array.length);
					for (int j = 0; j < replication_factor; j++){//to distribute replicas
						String node_id = data_node_queue.removeFirst();//next datablock in queue. this ensures replicas are on different nodes
						Host host = this.nodeId_host_map.get(node_id);							
						block_hosts.add(host);
						data_node_queue.addLast(node_id);
						to_send_nodes.add(node_id);
						file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(file,file_block),node_id);
						file_block.setBlockHosts(block_hosts);
						file_block.setBlockRange(new Tuple<Integer,Integer>(previous_block_end,line_no-1));
						file.getBlockIDMap().put(block_no, file_block);
						nodeId_block_map.get(node_id).add(file_block);
						this.block_file_map.put(new Tuple<String,Integer>(file_block.getFileName(),file_block.getBlockNumber()),block_file);
					}
					int replica_no = 1;
					//Here we actually send the replicas to the corresponding data_nodes using java RMI
					for (String nodeID : to_send_nodes){	
//						File copy = new File(file_block.getCustomPath(replica_no));
//						FileFunctions.copy(block_file, copy);
						DataNodeInterface current_data_node = (DataNodeInterface) this.main_registry.lookup(nodeID);
						current_data_node.initiateBlock(byte_array,file_block);
						replica_no++;
//						Files.delete(copy.toPath()); //delete the local replica
					}
					Files.delete(block_file.toPath());
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				System.out.println("DataNode not bound to NameNode yet!");
			}
		}
	}

	
	public void listFiles(){
		//To Do
	}
	
	
	public InetAddress getHost(){
		try {
			return InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	

	public ServerSocket getServerSocket() {
		return this.server_socket;
	}

	public void startHealthChecker(
			List<String> node_ids) {
		DFSHealthMonitor health_monitor = null;
		try {
			health_monitor = new DFSHealthMonitor(node_ids,InetAddress.getLocalHost(),port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		Thread th = new Thread(health_monitor);
		th.start();
	}

	public void returnFilesToNode(String nodeId) {
		File block_file;
		Tuple<String,Integer> tuple;
		DataNodeInterface current_data_node = null;
		try {
			current_data_node = (DataNodeInterface) this.main_registry.lookup(nodeId);
		} catch (AccessException e1) {
			e1.printStackTrace();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		} catch (NotBoundException e1) {
			e1.printStackTrace();
		}
		for (DFSBlock block : this.nodeId_block_map.get(nodeId)){
			DFSFile dfs_file = find_file(block.getFileName());
			tuple = new Tuple<String,Integer>(dfs_file.getFile().getName(),block.getBlockNumber());
			block_file = block_file_map.get(tuple);
			byte[] byte_array = new byte[(int) block_file.length()];
			FileInputStream fis;
			try {
				fis = new FileInputStream(block_file);
				BufferedInputStream bis = new BufferedInputStream(fis);
				bis.read(byte_array,0,byte_array.length);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				current_data_node.initiateBlock(byte_array, block);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
		}
	}

	

	
}
