package MapReduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Config.InternalConfig;
import DFS.DFSDataNode;
import DFS.DFSNameNodeInterface;
import Master.MapReduceMasterInterface;
import Util.FileFunctions;
import Util.Host;
import Util.JavaCustomClassLoader;
import Util.Tuple;


/**
 * Created by karansharma on 11/13/14.
 *
 * MapReducer is the object that handles interactions with the master map
 * reducer node. So if a user wants to map reduce a job, it first creates a
 * MapReducer object which will have a dataNode utilized for other active
 * jobs in the system.
 */
public class MapReducerClient {

	//TODO:
	//Make the user put text files and stuff into the files directory
	
    private DFSDataNode data_node;
    private Registry registry;
    private DFSNameNodeInterface name_node;
    private MapReduceMasterInterface master;
    private String map_reducer_id;
    private HashSet<String> jobIDs;
    String data_node_id;
    /*
       Constructor that connects to master node of MapReduce system.
       ParticipantID specifies the name of this user for future use.
     */
    public MapReducerClient(String participantID, Host master_host) throws Exception {
    	this.setMap_reducer_id(participantID);
    	jobIDs = new HashSet<String>();
    	InternalConfig.REGISTRY_HOST = master_host.hostname;

        /* Connect to Master Registry, and get name node and data node remote references */
    	registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
        name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
        master = (MapReduceMasterInterface) registry.lookup(InternalConfig.MAP_REDUCE_MASTER_ID);
        
        /* Locate an already existing data node */
        data_node_id = null;
        Set<String> all_node_ids = name_node.getNodeIds();
        ConcurrentHashMap<String,Host> data_node_hosts = name_node.getIdHostMap();
        for (String s : data_node_hosts.keySet()){
        	if (data_node_hosts.get(s).hostname.equals(InetAddress.getLocalHost().getHostName())){
        		data_node_id = s;
        	}
        }

    
        /* Construct and Start DFS dataNode layer (local) */
        InetAddress inet = InetAddress.getByName(name_node.getHost().getHostName());

        if (data_node_id == null){
        	throw new RuntimeException ("Must run a job from a machine already existing on the system");
        }
        

        /* Establishes connection to master */
    	master.handshakeWithSlave(participantID,data_node_id);
    }

    public void runJob(MapReducerConfig config, File[] files) throws Exception {
    	String mapper_name = config.getMapperClass().getName();
		String reducer_name = config.getReducerClass().getName();
		/* Example:  */
		Tuple<String,String> map_tuple = new Tuple<String,String>(mapper_name,mapper_name.replace('.', '/') + ".class");
		Tuple<String,String> red_tuple = new Tuple<String,String>(reducer_name, reducer_name.replace('.', '/') + ".class");
		
		/*Convert the map task to a byte array */
		Class<?> map_class = config.getMapperClass();
		String map_name = map_class.getName();
		String classAsPath_map = map_name.replace('.', '/') + ".class";
		InputStream stream = map_class.getClassLoader().getResourceAsStream(classAsPath_map);
		byte[] map_class_byte_array = FileFunctions.toByteArray(stream);
		/*Convert the reduce task to a byte array */
		Class<?> reduce_class = config.getReducerClass();
		String reduce_name = reduce_class.getName();
		String classAsPath_reduce = reduce_name.replace('.', '/') + ".class";
		InputStream stream1 = reduce_class.getClassLoader().getResourceAsStream(classAsPath_reduce);
		byte[] reduce_class_byte_array = FileFunctions.toByteArray(stream1);
		/* Create the job on the master and print out jobID for programmer to monitor */
        String jobID = master.createJob(map_reducer_id,InetAddress.getLocalHost().getHostName(),
        		config,map_class_byte_array,reduce_class_byte_array,map_tuple,red_tuple, config.getOutputFilePath());
        System.out.println("Job ID: " + jobID);
        Set<String> file_ids = SendFilesToNameNode(jobID, files);
        master.startJob(jobID,file_ids,config);
        jobIDs.add(jobID);
    }

	private Set<String> SendFilesToNameNode(String jobID, File[] files) {
        Set<String> file_ids = null;
		for (File file : files){
            System.out.println("Sending File: " + file);
            byte[] byte_array = new byte[(int) file.length()]; //assume that file is always small enough to fit
			FileInputStream fis;
			/*read the input file and convert to byte array for name node */
			try {
				fis = new FileInputStream(file);
				BufferedInputStream bis = new BufferedInputStream(fis);
				bis.read(byte_array,0,byte_array.length);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	try {
	    		/*passes map_reducer_id to coordinate DFSNameNode file_buffer flushes to respective MapReducers */
	    		name_node.bindFileFromByteArray(file.getName(),byte_array, jobID, this.data_node_id); 
	    	} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		try {
			file_ids = name_node.flushFilesToDataNodes(this.data_node_id);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
        return file_ids;
	}

	public String getMap_reducer_id() {
		return map_reducer_id;
	}

	public void setMap_reducer_id(String map_reducer_id) {
		this.map_reducer_id = map_reducer_id;
	}

	/*Starts a client interface for programmers to monitor progress of jobs */
	public void startInterface() {
		System.out.println("Starting " + this.map_reducer_id + " interface...");
		Scanner scanner = new Scanner(System.in);
		String usrInput;
		String[] args;
        /* Command Line Shell for client */
		while(true){
			System.out.print(this.map_reducer_id+ " -> ");
			usrInput = scanner.nextLine();
			args = usrInput.split(" ");
			if (args[0].toLowerCase().equals("job_details" )){
				try {
					/*Get the state of the job */
					String state = master.getState(args[1]);
					System.out.println(state);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

    
}
