package DFS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Util.Host;
import Util.Tuple;
import Config.ConfigSettings;

public class DFSNameNode implements Remote{
	public ConcurrentHashMap<String, List<DFSFile>> nodeId_file_map;
	public ConcurrentHashMap<DFSFile, String> file_directory_map;
	public HashMap<Tuple<DFSFile,DFSBlock>, String> file_block_host_map;
	public ConcurrentHashMap<String,Host> nodeId_host_map;
	
	public List<String> node_ids;
	
	public int port;
	public ServerSocket server_socket;
	
	public Registry datanode_registry;
	public String store_path;
	public List<DFSFile> all_dfsFiles;
	
	private String INIT_DIRECTORY = ConfigSettings.init_file_directory;
	private final int SPLIT_SIZE = ConfigSettings.split_size;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	
	
	
	
	public DFSNameNode(int port){
		node_ids = new ArrayList<String>();
		nodeId_file_map = new ConcurrentHashMap<String,List<DFSFile>>();
		file_directory_map = new ConcurrentHashMap<DFSFile, String>();
		file_block_host_map = new HashMap<Tuple<DFSFile,DFSBlock>, String>();
		nodeId_host_map  = new ConcurrentHashMap<String,Host>();
		all_dfsFiles = new ArrayList<DFSFile>();
		try {
			datanode_registry = LocateRegistry.createRegistry(REGISTRY_PORT);
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

	public ConcurrentHashMap<String,Host> getIdHostMap(){
		return this.nodeId_host_map;
	}

	@SuppressWarnings("resource")
	public void start(){
		System.out.println("Starting NameNode...");
		try {
			server_socket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		DFSConnectionManager connection_manager = new DFSConnectionManager(this);
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
				System.exit(0);
			}
			processCLInput(args);

			if (args[0].toLowerCase().equals("help" )){
				displayHelp();
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
				System.out.println(node_id + this.nodeId_host_map.get(node_id).hostname);
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
		
		else if (args[0].toLowerCase().equals("distribute_files")){
			this.partitionAndDistributeFiles();
		}
		else if (args[0].toLowerCase().equals("datanode_health?")){
			System.out.println("----------------");
			try {
				DFSHealthMonitor health_monitor = (DFSHealthMonitor) datanode_registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
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
		
		for (String node_id : this.node_ids){
			data_node_queue.addLast(node_id);
		}
		for (int i = 0; i < this.all_dfsFiles.size(); i++){
			DFSFile file = this.all_dfsFiles.get(i);
			String line;
			int line_no = 0;
			int block_no = 0;
			int previous_block_end = 0;
			BufferedReader br;
			BufferedWriter block_writer;
			DFSBlock file_block = new DFSBlock(file.getFile().getName(), block_no, new Tuple<Integer,Integer>(), null);
			Set<Host> block_hosts = new HashSet<Host>();
			int replication_factor = ConfigSettings.replication_factor;
			boolean isEmpty = true;
			try {
				File block_file = new File(file_block.getLocalBlockPath());
				br = new BufferedReader(new FileReader(file.getFile()));
				block_writer = new BufferedWriter(new FileWriter(block_file));
				while ((line = br.readLine()) != null){
					if (line_no % SPLIT_SIZE == 0 && line_no != 0){ 
						block_writer.close();
						block_hosts = new HashSet<Host>();
						file.getBlockIDMap().put(block_no, file_block);
						for (int j = 0; j < replication_factor; j++){
							String node_id = data_node_queue.removeFirst();
							Host host = this.nodeId_host_map.get(node_id);							
							block_hosts.add(host);
							data_node_queue.addLast(node_id);
							file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(file,file_block),node_id);
							DFSDataNode current_data_node = (DFSDataNode) this.datanode_registry.lookup(node_id);
							current_data_node.initiateBlock(block_file,file_block);
						}
						file_block.setBlockHosts(block_hosts);
						file_block.setBlockRange(new Tuple<Integer,Integer>(previous_block_end,line_no-1));
						isEmpty = true;
						previous_block_end = line_no;
						block_no++;
						file_block = new DFSBlock(file.getFile().getName(),block_no,new Tuple<Integer,Integer>(),null);
						block_file = new File(file_block.getLocalBlockPath());
						block_writer = new BufferedWriter(new FileWriter(file.getFile()));
					}
					block_writer.write(line + "\n");
					line_no ++;
					isEmpty = false;
				}
				
				if (isEmpty == false){
					block_writer.close();
					block_hosts = new HashSet<Host>();
					file.getBlockIDMap().put(block_no, file_block);
					for (int j = 0; j < replication_factor; j++){
						String node_id = data_node_queue.removeFirst();
						Host host = this.nodeId_host_map.get(node_id);							
						block_hosts.add(host);
						data_node_queue.addLast(node_id);
						file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(file,file_block),node_id);
						DFSDataNode current_data_node = (DFSDataNode) this.datanode_registry.lookup(node_id);
						current_data_node.initiateBlock(block_file,file_block);
					}
					file_block.setBlockHosts(block_hosts);
					file_block.setBlockRange(new Tuple<Integer,Integer>(previous_block_end,line_no-1));
			
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
		}
		Thread th = new Thread(health_monitor);
		th.start();
	}
	

	
}
