package DFS;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
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

import Config.ConfigSettings;
import Config.InternalConfig;
import Util.FileFunctions;
import Util.Host;
import Util.Tuple;

public class DFSNameNode extends UnicastRemoteObject implements DFSNameNodeInterface{
    /**
     *
     */
    private static final long serialVersionUID = 6201621654572231214L;
    public ConcurrentHashMap<Tuple<DFSFile,DFSBlock>, String> file_block_host_map; /* Key- (FileId, BlockNo), value- byte[] corresponding to file */
    public ConcurrentHashMap<String,Host> nodeId_host_map;
    public ConcurrentHashMap<String,Boolean> nodeId_active_map; /*Which nodes are active */
    public ConcurrentHashMap<String,List<DFSBlock>> nodeId_block_map;
    public ConcurrentHashMap<String,Set<DFSBlock>> fileID_block_map; /*File to corresponding block */
    public Set<String> node_ids;
    public int port;
    public ServerSocket server_socket;
    public Registry main_registry;
    public String store_path;
    public List<DFSFile> all_dfsFiles;

    private final int SPLIT_SIZE = ConfigSettings.split_size;
    private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
    public HashMap<Tuple<String,Integer>,byte[]> fileID_block_file_map;
    public ConcurrentHashMap<String,List<DFSFile>> slave_dfsfile_buffer; //before partitioning and sending out the files
    public ConcurrentHashMap<String, byte[]> fileID_byte_arr; /*Stores the fileId to the byte array corresponding to that file */
    private ConcurrentHashMap<String,Tuple<String,Integer>> data_nodeID_registry_info_map; /*maps data node id to its registry info */
    private int temp_free_port = InternalConfig.REGISTRY_PORT+1;


    public DFSNameNode(int port) throws RemoteException{
        node_ids = new HashSet<String>();/*All Node Ids */
        file_block_host_map = new ConcurrentHashMap<Tuple<DFSFile,DFSBlock>, String>(); /*File block to which host it is located on */
        nodeId_host_map  = new ConcurrentHashMap<String,Host>();/*NodeIds to their host */
        nodeId_active_map = new ConcurrentHashMap<String,Boolean>();/*State of node Id*/
        all_dfsFiles = new ArrayList<DFSFile>();/*All files on DFS */
        nodeId_block_map = new ConcurrentHashMap<String,List<DFSBlock>>(); /* What blocks each data node has */
        fileID_block_file_map = new HashMap<Tuple<String,Integer>,byte[]>(); /* byte[] representation of files */
        slave_dfsfile_buffer = new ConcurrentHashMap<String,List<DFSFile>>();/*Stores files in buffer when client is flushing files here*/
        fileID_block_map = new ConcurrentHashMap<String,Set<DFSBlock>>();/*All blocks corresponding to a file */
        fileID_byte_arr = new ConcurrentHashMap<String, byte[]>();
        data_nodeID_registry_info_map = new ConcurrentHashMap<String, Tuple<String,Integer>>(); /*Registry info for datanodes */
        this.port = port;
    }

    public Set<String> getNodeIds(){
        return this.node_ids;
    }


    /*Binds name node to registry */
    public void initRegistry() {
        try {
            main_registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, REGISTRY_PORT);
            main_registry.rebind(InternalConfig.NAME_NODE_ID, this);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

    }
    public int getPort(){
        return this.port;
    }

    /*Gets a free port for the data node to bind to */
    public int getFreeRegistryPort(){
        this.temp_free_port ++;
        return this.temp_free_port;
    }

    public ConcurrentHashMap<String,Tuple<String,Integer>> getDataNodeRegistryInfo(){
        return this.data_nodeID_registry_info_map;
    }

    public void addDataNodeRegistryInfo(String data_nodeId,
                                        Tuple<String, Integer> tuple) throws RemoteException{
        this.data_nodeID_registry_info_map.put(data_nodeId, tuple);

    }

    public ConcurrentHashMap<String,Set<DFSBlock>> getFileIDBlockMap(){
        return fileID_block_map;
    }

    /*When a dataNode is deemed inactive, their file replicas must be moved to other data nodes
     *to ensure that the replication factor is maintained
     */
    public void moveBlocksFromInactive(){
        System.out.println("Moving files from inactive dataNodes");
        System.out.println("Master ->");
        List<DFSBlock> unassociated_replica_blocks = new ArrayList<DFSBlock>();
        List<String> active_nodes = getActiveNodes();
        List<String> inactive_nodes = new ArrayList<String>();
		
		/*Look for all the inactive nodes, and get the blocks corresponding blocks to move */
        for (String node_id : this.nodeId_active_map.keySet()){
            if (nodeId_active_map.get(node_id) == false){
				/*Gets all replicas from the inactive datanode(s) */
                unassociated_replica_blocks.addAll(nodeId_block_map.get(node_id));
                inactive_nodes.add(node_id);
				/*Deletes the mapping from datanode to the blocks */
                removeInactiveNodeFromBlocks(node_id);
            }
        }
		/*FIFO to distribute replicas */
        LinkedList<String> queue = new LinkedList<String>();
        for (String node_id: active_nodes){
            queue.addLast(node_id);
        }
        String destination_node;
        DFSFile dfs_file;
        File block_file;
		/*Go through all the blocks that need to be moved */
        for (DFSBlock block : unassociated_replica_blocks){
            destination_node = queue.removeFirst();
            queue.addLast(destination_node);
			/*Looks through map for file*/
            dfs_file = find_file(block.getFileName());
            Tuple<String,Integer> tuple = new Tuple<String,Integer>(dfs_file.getDFSFile_id(),block.getBlockNumber());
			/*Get the byte[] corresponding to file to send to new datanode */
            byte[] byte_array = fileID_block_file_map.get(tuple);
			/*Adds the new mapping */
            file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(dfs_file,block),destination_node);
            block.getBlockHosts().add(this.nodeId_host_map.get(destination_node));
            try {
				/*Get the data node registry to initiate block on that data node */
                String registry_host = this.data_nodeID_registry_info_map.get(destination_node).getFirst();
                int registry_port = this.data_nodeID_registry_info_map.get(destination_node).getSecond();
                Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
                DataNodeInterface current_data_node = (DataNodeInterface) data_node_registry.lookup(destination_node);
                current_data_node.initiateBlock(byte_array, block,dfs_file.getDFSFile_id());
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
            RemoveNodesFromHostMap(node_id);
            nodeId_host_map.remove(node_id);
            nodeId_active_map.remove(node_id);
            nodeId_block_map.remove(node_id);

        }
    }

    private void removeInactiveNodeFromBlocks(String node_id) {
        for (DFSBlock block : nodeId_block_map.get(node_id)){
            block.getBlockHosts().remove(node_id);
        }
    }


    private void RemoveNodesFromHostMap(String node_id) {
        for (Tuple<DFSFile, DFSBlock> tuple: this.file_block_host_map.keySet()){
            if (file_block_host_map.get(tuple).equals(node_id)){
                file_block_host_map.remove(tuple);
            }
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

    public ConcurrentHashMap<Tuple<DFSFile,DFSBlock>, String> getBlockHostMap(){
        return this.file_block_host_map;
    }

    public ConcurrentHashMap<String, List<DFSBlock>> getIdBlockMap(){
        return this.nodeId_block_map;
    }

    /*Safely quits the name node, and exists all other connections
     *Clears out all the scruf from the system
     */
    public void quit(){
        try {
            ConnectionManagerInterface c_manager = (ConnectionManagerInterface) this.main_registry.lookup(InternalConfig.CONNECTION_MANAGER_ID);
            c_manager.setActive(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (DFSFile file: this.all_dfsFiles){
            file.getFile().delete();
        }
        closeDataNodes();
        FileFunctions.deleteDirectory(new File(InternalConfig.DFS_STORAGE_PATH)); /*Clears Scruf */
        try {
            this.server_socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    @SuppressWarnings("resource")
    public void start(){
        System.out.println("Starting NameNode...");
        try {
            server_socket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*
            Starts Connection manager in background thread,
            which listens for dataNode connections.
         */
        DFSConnectionManager connection_manager = null;
        try {
            connection_manager = new DFSConnectionManager(this);
        } catch (RemoteException e1) {
            e1.printStackTrace();
        }
        Thread connection_thread = new Thread(connection_manager);
        connection_thread.start();

    }

    private void closeDataNodes() {
        /*Safely close each dataNode*/
        for (String node_id: this.node_ids){
            try {
                DataNodeInterface data_node = (DataNodeInterface) this.main_registry.lookup(node_id);
                data_node.exitDataNode();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /*Partitions and distributes files*/
    public void partitionAndDistributeFiles(List<DFSFile> list) throws IOException{
		/*FIFO used to distribute files */
        LinkedList<String> data_node_queue = new LinkedList<String>();
        String line;
        BufferedReader br;
        int replication_factor = ConfigSettings.replication_factor;
		/*Set up queue to cycle replicas and files across DFS */
        for (String node_id : this.node_ids){
            data_node_queue.addLast(node_id);
        }
        //Loop through all the files that need to be distributed
        for (int i = 0; i < list.size(); i++){
			/*Sets up partition, and creates first block */
            DFSFile file = list.get(i);
            int line_no = 0;
            int block_no = 0;
            int previous_block_end = 0;
			/*Create a new block for the file to be sent*/
            DFSBlock file_block = new DFSBlock(file.getFile().getName(),file.getDFSFile_id(),
                    block_no, new Tuple<Integer,Integer>(), null);
            Set<DFSBlock> all_blocks = new HashSet<DFSBlock>();
            Set<Host> block_hosts = new HashSet<Host>();
            List<String> to_send_nodes = new ArrayList<String>();
            boolean isEmpty = true;
			/*-set up byte array to be written too
			 *-assumes all lines are off equal length per file (same no. of bytes)
			 */
            int line_count = FileFunctions.countLines(file.getFile().getName());
            int line_length = (int) (file.getFile().length()/line_count);
            byte[] byte_array; //= new byte[line_length*SPLIT_SIZE];
            ByteArrayOutputStream bos = new ByteArrayOutputStream(line_length*SPLIT_SIZE);
            byte[] file_byte_array = this.fileID_byte_arr.get(file.getDFSFile_id());
            try {
                br = new BufferedReader(new FileReader(file.getFile()));
				/*Loop through all lines of the input file */
                while ((line = br.readLine()) != null){
					/* This is when we need to split the block off to send
					 * SPLIT_SIZE is defined in internal config. It is the number of lines
					 * that will be contained in one block
					 */
                    if (line_no % SPLIT_SIZE == 0 && line_no != 0){
						/*Maintain which hosts the block replicas are being sent to */
                        block_hosts = new HashSet<Host>();
                        file.getBlockIDMap().put(block_no, file_block);
                        byte_array = bos.toByteArray();
						/*Number of times the block has to be replicated */
                        for (int j = 0; j < replication_factor; j++){//to distribute replicas
                            String node_id = data_node_queue.removeFirst();//next datablock in queue. this ensures replicas are on different nodes
                            Host host = this.nodeId_host_map.get(node_id);
							/*Pick a host to send to from queue, then enqueue same host */
                            block_hosts.add(host);
                            data_node_queue.addLast(node_id);
                            to_send_nodes.add(node_id);
							
							/* Update required mappings */
                            file_block_host_map.put(new Tuple<DFSFile, DFSBlock>(file,file_block),node_id);
                            file_block.setBlockHosts(block_hosts);
                            file_block.setBlockRange(new Tuple<Integer,Integer>(previous_block_end,line_no-1));
                            file.getBlockIDMap().put(block_no, file_block);
                            nodeId_block_map.get(node_id).add(file_block);
							/*store byte_array to corresponding block */
                            this.fileID_block_file_map.put(new Tuple<String,Integer>(file.getDFSFile_id(),
                                    file_block.getBlockNumber()),byte_array);

                        }
                        all_blocks.add(file_block);
                        //Here we actually send the replicas to the corresponding data_nodes using java RMI
					
						/*Send block replicas to hosts */
                        for (String nodeID : to_send_nodes){
							/*get data node registry*/
                            String registry_host = this.data_nodeID_registry_info_map.get(nodeID).getFirst();
                            int registry_port = this.data_nodeID_registry_info_map.get(nodeID).getSecond();
                            Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
                            DataNodeInterface current_data_node = (DataNodeInterface) data_node_registry.lookup(nodeID);
							
							/*Initiate byte_array to data node */
                            current_data_node.initiateBlock(byte_array,file_block,file.getDFSFile_id());
                        }
                        to_send_nodes = new ArrayList<String>();
                        isEmpty = true;
                        previous_block_end = line_no;
                        block_no++;
						/*Create new block to be written */
                        file_block = new DFSBlock(file.getFile().getName(),file.getDFSFile_id(),
                                block_no,new Tuple<Integer,Integer>(),null);
                        bos = new ByteArrayOutputStream(line_length*SPLIT_SIZE);
                    }
					/*This converts the line to a byte array, and copies it to a ByteArrayOutputStream */
                    byte[] line_byte_array = line.getBytes();
                    int index;
					/*Ensures there are no out of bounds exceptions */
                    if (line_no % SPLIT_SIZE == 0){
                        index = 0;
                    }
                    else{
                        index = ((line_no % SPLIT_SIZE) * line_length) -1;
                    }
                    bos.write(line_byte_array, 0,line_byte_array.length);
                    bos.write((byte) '\n'); /*Write new line character to byte array */
                    bos.flush();
                    line_no ++;
                    isEmpty = false;
                }
				/*goes here on the last byte array to be sent 
				 * Follows exact same pattern as above code. See comments above for further detail.
				 * */
                if (isEmpty == false){
                    br.close();
                    block_hosts = new HashSet<Host>();
                    file.getBlockIDMap().put(block_no, file_block);
                    byte_array = bos.toByteArray();
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
                        this.fileID_block_file_map.put(new Tuple<String,Integer>(file.getDFSFile_id(),file_block.getBlockNumber()),byte_array);
                    }
                    all_blocks.add(file_block);
                    //Here we actually send the replicas to the corresponding data_nodes using java RMI
                    for (String nodeID : to_send_nodes){
                        String registry_host = this.data_nodeID_registry_info_map.get(nodeID).getFirst();
                        int registry_port = this.data_nodeID_registry_info_map.get(nodeID).getSecond();
                        Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
                        DataNodeInterface current_data_node = (DataNodeInterface) data_node_registry.lookup(nodeID);
                        current_data_node.initiateBlock(byte_array,file_block,file.getDFSFile_id());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NotBoundException e) {
                System.out.println("DataNode not bound to NameNode yet!");
            }
            this.fileID_block_map.put(file.getDFSFile_id(),all_blocks);
            all_blocks = new HashSet<DFSBlock>();
        }
    }


    public void listFiles(){

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

    /* Constructs DFSHealthMonitor with given dataNodes */
    public void startHealthChecker(List<String> node_ids) {
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

    /* On data node failure and resurrection, the files will be returned to the data node
     * This code is reached if the data node is revived before it is compeletely inactive (health <= 0)
     * Allows us to return files to node
     * */
    public void returnFilesToNode(String nodeId) {
        Tuple<String,Integer> tuple;
        DataNodeInterface current_data_node = null;
        try {
			/*Get the data node registry */
            String registry_host = this.data_nodeID_registry_info_map.get(nodeId).getFirst();
            int registry_port = this.data_nodeID_registry_info_map.get(nodeId).getSecond();
            Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
            current_data_node = (DataNodeInterface) data_node_registry.lookup(nodeId);
        } catch (AccessException e1) {
            e1.printStackTrace();
        } catch (RemoteException e1) {
            e1.printStackTrace();
        } catch (NotBoundException e1) {
            e1.printStackTrace();
        }
		/*Resend the file blocks to the data node */
        for (DFSBlock block : this.nodeId_block_map.get(nodeId)){
            DFSFile dfs_file = find_file(block.getFileName());
            tuple = new Tuple<String,Integer>(dfs_file.getDFSFile_id(),block.getBlockNumber());
            byte[] byte_array =  fileID_block_file_map.get(tuple);
            try {
                current_data_node.initiateBlock(byte_array, block,dfs_file.getDFSFile_id());
            } catch (RemoteException e) {
                e.printStackTrace();
            }

        }
    }


    /**
     *On a slave being created, the client provides a file that is sent to the
     *name node to be distributed to the data nodes.
     */
    @Override
    public void bindFileFromByteArray(String name,byte[] byte_array,String job_id,String datanode_id) throws RemoteException {
        File file = new File(name);
        FileOutputStream fos;
        try {
			/*Receive files from MapReducer and create DFSFile */
            fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            bos.write(byte_array, 0, byte_array.length);
            bos.flush();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        DFSFile new_dfs_file = new DFSFile(file,job_id); /*Construct dfs file with unique id */
        this.fileID_byte_arr.put(new_dfs_file.getDFSFile_id(), byte_array);
        this.all_dfsFiles.add(new_dfs_file);
		/*Add to the buffer before partitioning. If hashmap already contains then append, else create new list 
		 *This map stores which slaves sent which dfs file
		 * */
        if (this.slave_dfsfile_buffer.containsKey(datanode_id) == true){
            List<DFSFile> buffer = this.slave_dfsfile_buffer.get(datanode_id);
            buffer.add(new_dfs_file);
            this.slave_dfsfile_buffer.put(datanode_id, buffer);
        }
        else{
            List<DFSFile> buffer = new ArrayList<DFSFile>();
            buffer.add(new_dfs_file);
            this.slave_dfsfile_buffer.put(datanode_id,buffer);
        }

    }

    @Override
    public Set<String> flushFilesToDataNodes(String datanode_id) {

        try {
            this.partitionAndDistributeFiles(this.slave_dfsfile_buffer.get(datanode_id));
        } catch (IOException e) {
            e.printStackTrace();
        }
		/* Get all the file ids to return back to map reducer */
        Set<String> file_ids = new HashSet<String>();
        for (DFSFile dfs_file: this.slave_dfsfile_buffer.get(datanode_id)){
            file_ids.add(dfs_file.getDFSFile_id());
        }
        this.slave_dfsfile_buffer.put(datanode_id,new ArrayList<DFSFile>());
        return file_ids;
    }


}
