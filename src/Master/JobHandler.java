package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSNameNodeInterface;
import MapReduce.TaskManagerInterface;
import Util.Host;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


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
   private HashMap<String,TaskManagerInterface> taskManagers;
   private HashSet<String> failedNodes;
   private ConcurrentHashMap<String,byte[]> reduceOutputs;
   private String callerHost;
   private String outFilePath;


   public JobHandler(String jobID, String callerHost, String outFilePath)
   {
       this.jobID = jobID;
       partitions = new HashMap<>();
       taskManagers = new HashMap<>();
       failedNodes = new HashSet<>();
       reduceOutputs = new ConcurrentHashMap<>();
       this.callerHost = callerHost;
       this.outFilePath = outFilePath;
   }

   public void start(Set<String> file_ids) throws Exception{
       this.active = true;
       registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
       name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
       scheduler = (ScheduleManagerInterface) registry.lookup("Scheduler");


       //Select a replica to map to for each block through scheduler
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

                //Store by which host replica is at
                partitions.put(hostName,blocks);
           }
       }

       /* Send tasks to appropriate TaskManagers */
       Set<String> hosts = partitions.keySet();
       ConcurrentHashMap<String, Host> idToHostMap = name_node.getIdHostMap();
       for(String host : hosts)
       {
    	   String data_node_id = findDataNodeID(idToHostMap, host);
    	   String registry_host = name_node.getDataNodeRegistryInfo().get(data_node_id).getFirst();
    	   int registry_port = name_node.getDataNodeRegistryInfo().get(data_node_id).getSecond();

		   /*Get the data node registry */
    	   Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
    	   TaskManagerInterface taskManagerInterface = (TaskManagerInterface)
                   data_node_registry.lookup(InternalConfig.generateTaskManagerId(host));
    	   taskManagerInterface.addJob(jobID,partitions.get(host));
           taskManagers.put(host,taskManagerInterface);
       }
   }
   
	private String findDataNodeID(ConcurrentHashMap<String, Host> idToHostMap, String host) {
		for (String s : idToHostMap.keySet()){
			if (idToHostMap.get(s).hostname.equals(host)){
				return s;
			}
		}
		return null;
	}

   public boolean getActive() {return this.active;}

   public String getState() throws Exception {

        String state = "";
        if(partitions.size() == 0) {
            return "Job not yet scheduled.";
        }

        /* Get job state from each host */
        for(String host : partitions.keySet())
        {
            int mapsLeft = taskManagers.get(host).mapsLeft(jobID);
            String nodeID = taskManagers.get(host).getDataNodeID();


            if(failedNodes.contains(nodeID))
            {
                state = state + "Failure during execution on " + nodeID + "\n";
            }
            else if (mapsLeft < 0)
            {
                state = state + "All tasks completed on " + nodeID + "\n";
            }
            else if(mapsLeft == 0)
            {
                state = state + "Map tasks completed and reduce running on " + nodeID + "\n";
            }
            else
            {
                state = state + mapsLeft + "map tasks still running or queued on " + nodeID + "\n";
            }
        }
        return state;
    }

    public void nodeFailed(String nodeID){
        failedNodes.add(nodeID);
    }

    public synchronized void nodeCompleted(String nodeID, byte[] output) throws Exception
    {
        /* Store reduce output */
        reduceOutputs.put(nodeID,output);

        /* Check if Job Complete */
        if (reduceOutputs.size() == partitions.size())
        {
            String registry_host = name_node.getDataNodeRegistryInfo().get(nodeID).getFirst();
            int registry_port = name_node.getDataNodeRegistryInfo().get(nodeID).getSecond();

		    /*Get the data node registry */
            Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
            TaskManagerInterface taskManagerInterface = (TaskManagerInterface)
                    data_node_registry.lookup(InternalConfig.generateTaskManagerId(callerHost));


            /* Tell TaskManager to write files */
            taskManagerInterface.writeMROutput(reduceOutputs,outFilePath);

        }
    }

}
