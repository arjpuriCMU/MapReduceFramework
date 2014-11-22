package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSNameNodeInterface;
import MapReduce.TaskManager;
import MapReduce.TaskManagerInterface;
import Util.Host;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karansharma on 11/17/14.
 */
public class ScheduleManager extends UnicastRemoteObject implements ScheduleManagerInterface, Runnable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//Queue<DFSBlock>
    Registry registry;
    public ScheduleManager() throws RemoteException
    {
        try {
            registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public void run()
    {

    }

    /* Returns hostName of replica to map to */
    public String selectReplica(String jobID, DFSBlock block) throws RemoteException{
        Set<Host> replicaHosts = block.getBlockHosts();
        int minLoad = -1;
        String minHostName = "";
        /*Get the name node from the central registry */
    	DFSNameNodeInterface name_node = null;
		try {
			name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
		} catch (NotBoundException e1) {
			e1.printStackTrace();
		}
    	/* Get the corresponding data node's registry info */
    	ConcurrentHashMap<String, Host> idToHostMap = name_node.getIdHostMap();
        for(Host replicaHost : replicaHosts)
        {
            TaskManagerInterface taskManager = null;
            try {
            	String data_node_id = findDataNodeID(idToHostMap, replicaHost);
            	String registry_host = name_node.getDataNodeRegistryInfo().get(data_node_id).getFirst();
    			int registry_port = name_node.getDataNodeRegistryInfo().get(data_node_id).getSecond();
    			/*Get the data node registry */
    			Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
                taskManager =
                    (TaskManagerInterface) data_node_registry.lookup(InternalConfig.generateTaskManagerId(replicaHost.hostname));
            } catch (RemoteException e) {
                e.printStackTrace();
            } catch (NotBoundException e) {
                e.printStackTrace();
            }
            int load = taskManager.taskLoad();

            if(minLoad < 0 || load < minLoad)
            {
                minLoad = load;
                minHostName = replicaHost.hostname;
            }
        }
        return minHostName;
    }

	private String findDataNodeID(ConcurrentHashMap<String, Host> idToHostMap, Host replicaHost) {
		for (String s : idToHostMap.keySet()){
			if (idToHostMap.get(s).equals(replicaHost)){
				return s;
			}
		}
		return null;
	}
}
