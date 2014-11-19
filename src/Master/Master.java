package Master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;
import Config.InternalConfig;
import DFS.DFSNameNode;
import MapReduce.MapReducerConfig;
import Util.Host;
import Util.Tuple;

public class Master extends UnicastRemoteObject implements MapReduceMasterInterface {
	private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
	private int port;
	private Host name_node_host;
	private Registry main_registry;
	private DFSNameNode name_node;
	private Set<String> slave_ids;
	private ConcurrentHashMap<String,String> slave_id_datanode_id_map;
    private ConcurrentHashMap<String,JobHandler> jobs;
    private ScheduleManager scheduleManager;
	private ConcurrentHashMap<String,byte[]> map_class_byte_array_map;
	private ConcurrentHashMap<String,byte[]> reduce_class_byte_array_map;
	
	public Master(int port) throws RemoteException{
		slave_ids = new HashSet<String>();
		map_class_byte_array_map = new ConcurrentHashMap<String,byte[]>();
		reduce_class_byte_array_map = new ConcurrentHashMap<String,byte[]>();
		
		this.port = port;
		try {
			InternalConfig.REGISTRY_HOST = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		System.out.println("NameNode being initiated.");
		name_node = new DFSNameNode(port);
		name_node.start();

		try {
			main_registry = LocateRegistry.createRegistry(REGISTRY_PORT);
			main_registry.bind(InternalConfig.MAP_REDUCE_MASTER_ID, this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
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

	public Host getName_node_host() {
		return name_node_host;
	}


	public void setName_node_host(Host name_node_host) {
		this.name_node_host = name_node_host;
	}

	@Override
	public void handshakeWithSlave(String participantID,String slave_id)
			throws RemoteException {
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
		map_class_byte_array_map.put(InternalConfig.generateTaskId(mapper_name[mapper_name.length-1], jobID),map_class_byte_array);
		reduce_class_byte_array_map.put(InternalConfig.generateTaskId(reducer_name[reducer_name.length-1], jobID), reduce_class_byte_array);
		return jobID;
    }

    public void startJob(String jobID,Set<String> file_ids, MapReducerConfig config) throws Exception{
        jobs.get(jobID).start(file_ids);
    }


	
}
