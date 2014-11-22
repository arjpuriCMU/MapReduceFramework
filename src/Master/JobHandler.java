package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSNameNode;
import DFS.DFSNameNodeInterface;
import MapReduce.TaskManager;
import MapReduce.TaskManagerInterface;
import Util.Host;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;


/**
 * Created by karansharma on 11/17/14.
 */
public class JobHandler {

   private String jobID;
   private boolean active = false;
   private Registry registry;
   private ScheduleManagerInterface scheduler;
   private DFSNameNodeInterface name_node;
   private HashMap<String,Set<DFSBlock>> partitions;

   public JobHandler(String jobID)
   {
       this.jobID = jobID;
       partitions = new HashMap<String,Set<DFSBlock>>();
   }

   public void start(Set<String> file_ids) throws Exception{
       this.active = true;
       registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
       name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
       scheduler = (ScheduleManagerInterface) registry.lookup("Scheduler");


       //Select replicas of each block through scheduler
       for(String file_id : file_ids)
       {
           Set<DFSBlock> dfsBlocks = name_node.getFileIDBlockMap().get(file_id);
           for(DFSBlock dfsBlock : dfsBlocks)
           {
                String hostName = scheduler.selectReplica(jobID,dfsBlock);
                HashSet<DFSBlock> blocks = null;

                if(partitions.containsKey(hostName)) //partition exists
                    blocks = (HashSet<DFSBlock>) partitions.get(hostName);
                else //new partition
                    blocks = new HashSet<DFSBlock>();

                blocks.add(dfsBlock);
                partitions.put(hostName,blocks);
           }
       }

       /* Send tasks to appropriate TaskManagers */
       Set<String> hosts = partitions.keySet();
       ConcurrentHashMap<String, Host> idToHostMap = name_node.getIdHostMap();
       for(String host : hosts){
    	   System.out.println(host);
    	   String data_node_id = findDataNodeID(idToHostMap, host);
    	   String registry_host = name_node.getDataNodeRegistryInfo().get(data_node_id).getFirst();
    	   int registry_port = name_node.getDataNodeRegistryInfo().get(data_node_id).getSecond();
		/*Get the data node registry */
    	   Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
    	   TaskManagerInterface taskManagerInterface = (TaskManagerInterface) data_node_registry.lookup(InternalConfig.generateTaskManagerId(host));
    	   taskManagerInterface.addJob(jobID,partitions.get(host));
       }
   }
   
	private String findDataNodeID(ConcurrentHashMap<String, Host> idToHostMap, String host) {
		for (String s : idToHostMap.keySet()){
			if (idToHostMap.get(s).hostname.equals(host)){
				return s;
			}
		}
		System.out.println("izzz NULL!");
		return null;
	}

   public boolean getActive() {return this.active;}

}
