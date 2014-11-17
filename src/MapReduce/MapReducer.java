package MapReduce;

import Config.InternalConfig;
import DFS.DFSDataNode;
import DFS.DFSNameNodeInterface;
import DFS.HealthMonitor;
import Master.MapReduceMasterInterface;
import Util.Host;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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

    /* Constructor that connects to master node of MapReduce system.
       ParticipantID specifies the name of this user for future use.
     */
    public MapReducer(String participantID, Host master_host) throws Exception {

        /* Connect to Master Registry, and get name node and data node remote references */
    	registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
        name_node= (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
        master = (MapReduceMasterInterface) registry.lookup(InternalConfig.MAP_REDUCE_MASTER_ID);

        /* Construct and Start DFS dataNode layer (local) */
        InetAddress inet = InetAddress.getByName(name_node.getHost().getHostName());
        String data_node_id = InternalConfig.generateDataNodeId(participantID);
        data_node = new DFSDataNode(data_node_id, inet, master_host.port);
        data_node.start();

        /* Establishes connection to master */
    	master.handshakeWithSlave(participantID,data_node_id);
    }

    public void runJob(MapReduceInterface jobClass, File[] files) throws Exception{
        String jobID = "some shit";
        registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);

    	SendFilesToNameNode(jobID,files);
    }

	private String[] SendFilesToNameNode(String jobID, File[] files) {
        String[] fileIDs = null;
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
	    		DFSNameNodeInterface name_node= (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
	    		name_node.bindFileFromByteArray(byte_array, file.getName());
	    	} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			}
			
		}
        return fileIDs;
	}
    
    
}
