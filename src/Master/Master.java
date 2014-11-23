package Master;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteObject;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;
import Config.InternalConfig;
import DFS.ConnectionManagerInterface;
import DFS.DFSBlock;
import DFS.DFSConnectionManager;
import DFS.DFSDataNode;
import DFS.DFSFile;
import DFS.DFSNameNode;
import DFS.DataNodeInterface;
import DFS.HealthMonitor;
import MapReduce.MapReducerConfig;
import Util.FileFunctions;
import Util.Host;
import Util.Tuple;

public class Master extends UnicastRemoteObject implements MapReduceMasterInterface {
	
	private static final long serialVersionUID = 1L;
	private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
	private int port;
	private Host name_node_host;
	private Registry main_registry;
	private DFSNameNode name_node;
	private Set<String> slave_ids;
	private ConcurrentHashMap<String,String> slave_id_datanode_id_map;
    private ConcurrentHashMap<String,JobHandler> jobs;
    private ScheduleManager scheduleManager;
	private ConcurrentHashMap<String,Tuple<byte[],byte[]>> jobId_class_byte_array_map;
	private ConcurrentHashMap<String,Tuple<String,String>> jobId_mapname_map;
	private String host;
	
	
	public Master(int port) throws RemoteException{
		slave_ids = new HashSet<String>();
		jobId_class_byte_array_map = new ConcurrentHashMap<String,Tuple<byte[],byte[]>>();
		this.jobId_mapname_map = new ConcurrentHashMap<String,Tuple<String,String>>();
		this.jobs = new ConcurrentHashMap<String,JobHandler>();
		this.port = port;
		try {
			this.host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e2) {
			e2.printStackTrace();
		}
		try {
			InternalConfig.REGISTRY_HOST = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		System.out.println("NameNode being initiated.");
		try {
			main_registry = LocateRegistry.createRegistry(REGISTRY_PORT);
			main_registry.bind(InternalConfig.MAP_REDUCE_MASTER_ID, this);
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
		name_node = new DFSNameNode(port);
		name_node.start();
		name_node.initRegistry();
	
		setName_node_host(new Host(name_node.getHost().getHostName(),port));
		slave_id_datanode_id_map = new ConcurrentHashMap<String,String>();
        /* Start Schedule Manager and put it in registry*/
        scheduleManager = new ScheduleManager();
        Thread schedulerThread = new Thread(scheduleManager);
        schedulerThread.start();
        try {
            main_registry.bind("Scheduler",scheduleManager);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }
    }
	
	public String getMasterHost(){
		return this.host;
	}

	public Host getName_node_host() {
		return name_node_host;
	}

	public void setName_node_host(Host name_node_host) {
		this.name_node_host = name_node_host;
	}
	
	public ConcurrentHashMap<String,Tuple<byte[],byte[]>> getClassMap(){
		return this.jobId_class_byte_array_map;
	}
	
	public ConcurrentHashMap<String,Tuple<String,String>> getClassNameMap(){
		return this.jobId_mapname_map;
	}

	@Override
	public void handshakeWithSlave(String participantID,String slave_id)
			throws RemoteException {
		System.out.println(slave_id + " added to master");
		slave_ids.add(participantID);
		slave_id_datanode_id_map.put(participantID, slave_id);
	}

    /* TODO: figure out how to pass jar file */
    /* Creates JobHandler */
    public String createJob(String participantID,MapReducerConfig config, byte[] map_class_byte_array, 
    		byte[] reduce_class_byte_array, Tuple<String, String> map_tuple, Tuple<String, String> red_tuple){
        String jobID = participantID + config;
        
        while(jobs.containsKey(jobID))
        {
            jobID+= "0";
        }
        JobHandler jobHandler = new JobHandler(jobID);
        jobs.put(jobID,jobHandler);
        /*Ex. WordCount.WordCountMapper -> [WordCount,WordCountMapper] */
        String[] mapper_name = (map_tuple.getFirst().split("\\."));
		String[] reducer_name = (red_tuple.getFirst().split("\\."));
		/* UPLOAD_PATH + WordCountMapper-asdfsa.class */
		String map_path = ConfigSettings.UPLOAD_PATH + mapper_name[mapper_name.length-1] + "-" + jobID + ".class";
		String reduce_path = ConfigSettings.UPLOAD_PATH + reducer_name[reducer_name.length-1] + "-" + jobID + ".class";
		jobId_class_byte_array_map.put(jobID,new Tuple<byte[],byte[]>(map_class_byte_array,reduce_class_byte_array));
		this.jobId_mapname_map.put(jobID, new Tuple<String,String>(map_tuple.getFirst(),red_tuple.getFirst()));
		return jobID;
    }

    public void startJob(String jobID,Set<String> file_ids, MapReducerConfig config){
        try {
			jobs.get(jobID).start(file_ids);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
	public void start(){
		System.out.println("Starting Master...");
		Scanner scanner = new Scanner(System.in);
		String usrInput;
		String[] args;
        /* TODO: NameNode launched from Master in foreground so may infinite loop */
        /* Command Line Shell for NameNode */
		while(true){
			System.out.print("Master -> ");
			usrInput = scanner.nextLine();
			args = usrInput.split(" ");

            /* If user quits nameNode */
			if (args[0].toLowerCase().equals("quit")){
				this.name_node.quit();
				System.exit(0);
			}

			processCLInput(args);
			if (args[0].toLowerCase().equals("help" )){
			}
		}
	}
	private void processCLInput(String[] args) {
		if (args[0].toLowerCase().equals("data_nodes?")){ //Display all running workers
					
		}
		else if (args[0].toLowerCase().equals("files?")){
		}
		
		else if (args[0].toLowerCase().equals("file_blocks?")){
		}

        /* Distributes Files */
		else if (args[0].toLowerCase().equals("datanode_health?")){

		}
		else if (args[0].toLowerCase().equals("host?")){

		}
	}

    /* returns String with job information */
    public String getState(String jobID) throws Exception
    {
        if(!jobs.containsKey(jobID))
            return "Not a valid JobID";
        return jobs.get(jobID).getState();
    }

    /* Called by TaskManager to report job failure */
    public void jobFailure(String jobID, String dataNodeID)
    {
        jobs.get(jobID).nodeFailed(dataNodeID);
    }

    /* Called by TaskManager to report job completion */
    public void jobCompleted(String jobID, String nodeID, byte[] output) {
        jobs.get(jobID).nodeCompleted(nodeID,output);
    }


	
}
