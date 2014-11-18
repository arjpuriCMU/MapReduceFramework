package MapReduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

import Config.InternalConfig;
import DFS.DFSDataNode;
import DFS.DFSNameNodeInterface;
import Master.MapReduceMasterInterface;
import Util.Host;

/**
 * Created by karansharma on 11/13/14.
 *
 * MapReducer is the object that handles interactions with the master map
 * reducer node. So if a user wants to map reduce a job, it first creates a
 * MapReducer object which will have a dataNode utilized for other active
 * jobs in the system. When the user wants to run a job on this MapReducer,
 * a class extending the MapReduceInterface must be created and then sent
 * to runJob along with the input files of the job.
 */
public class MapReducer {

	//TO DO:
	//change registry to master's host, and instantiate registry on master
	//add name node host to master
	//this data node has to get name node host for registry
	
    private DFSDataNode data_node;
    private Registry registry;
    private DFSNameNodeInterface name_node;
    private MapReduceMasterInterface master;
    private String map_reducer_id;
    private HashSet<String> jobIDs;
    private TaskManager task_manager;

    /* Constructor that connects to master node of MapReduce system.
       ParticipantID specifies the name of this user for future use.
     */
    public MapReducer(String participantID, Host master_host) throws Exception {
    	this.setMap_reducer_id(participantID);
        /* Connect to Master Registry, and get name node and data node remote references */
    	registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
        name_node= (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
        master = (MapReduceMasterInterface) registry.lookup(InternalConfig.MAP_REDUCE_MASTER_ID);

        /* Construct and Start DFS dataNode layer (local) */
        InetAddress inet = InetAddress.getByName(name_node.getHost().getHostName());
        String data_node_id = InternalConfig.generateDataNodeId(participantID);
        data_node = new DFSDataNode(data_node_id, inet, master_host.port);
        data_node.start();
        task_manager = new TaskManager(data_node_id,Runtime.getRuntime().availableProcessors());
        (new Thread(task_manager)).start();

        /* Establishes connection to master */
    	master.handshakeWithSlave(participantID,data_node_id);
    }

    //TODO: Jar file containing MapReduceInterface
    public void runJob(String JarFileName, File[] files) throws Exception {
        String jobID = master.createJob(map_reducer_id,JarFileName);
        SendFilesToNameNode(jobID, files);
        master.startJob(jobID);
        jobIDs.add(jobID);
    }

	private void SendFilesToNameNode(String jobID, File[] files) {
		for (File file : files){
			byte[] byte_array = new byte[(int) file.length()]; //assume that file is always small enough to fit
			FileInputStream fis;
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
	    		name_node.bindFileFromByteArray(file.getName(),byte_array, jobID, this.map_reducer_id); 
	    	} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		try {
			Set<String> file_ids = name_node.flushFilesToDataNodes(this.map_reducer_id);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
        return;
	}

	public String getMap_reducer_id() {
		return map_reducer_id;
	}

	public void setMap_reducer_id(String map_reducer_id) {
		this.map_reducer_id = map_reducer_id;
	}

    
}
