package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSNameNode;
import DFS.DFSNameNodeInterface;
import MapReduce.TaskManager;
import MapReduce.TaskManagerInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
   private String JarFile;

   public JobHandler(String jobID)
   {
       this.jobID = jobID;
       partitions = new HashMap<>();
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
                    blocks = (HashSet) partitions.get(hostName);
                else //new partition
                    blocks = new HashSet<DFSBlock>();

                blocks.add(dfsBlock);
                partitions.put(hostName,blocks);
           }
       }

       /* Send tasks to appropriate TaskManagers */
       Set<String> hosts = partitions.keySet();
       for(String host : hosts)
       {
           TaskManagerInterface taskManagerInterface =
                   (TaskManagerInterface) registry.lookup(InternalConfig.generateTaskManagerId(host));
           taskManagerInterface.addJob(jobID,partitions.get(host));
       }
   }

   public boolean getActive() {return this.active;}

}
