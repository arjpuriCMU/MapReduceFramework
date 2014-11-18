package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSNameNode;
import DFS.DFSNameNodeInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Set;


/**
 * Created by karansharma on 11/17/14.
 */
public class JobHandler {

   private String jobID;
   private boolean active = false;
   private Registry registry;
   private ScheduleManagerInterface scheduler;
   private DFSNameNodeInterface name_node;

   public JobHandler(String jobID)
   {
       this.jobID = jobID;
   }

   public void start() throws Exception{
       this.active = true;
       registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
       name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
       scheduler = (ScheduleManagerInterface) registry.lookup("Scheduler");


       List<DFSBlock> blocksSet = name_node.getIdBlockMap().get(jobID);

       for(DFSBlock block : blocksSet)
       {

           /* Try to run map operation for each block */

       }
   }

   public boolean getActive() {return this.active;}

}
