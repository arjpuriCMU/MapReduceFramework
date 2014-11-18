package Master;

import Config.InternalConfig;
import DFS.DFSNameNode;
import DFS.DFSNameNodeInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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



       //for(String file : fileIDs)
       {
           /* Find all blocks of file */
           /* Try to run map operation for each block */

       }
   }

   public boolean getActive() {return this.active;}

}
