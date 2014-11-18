package Master;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Config.InternalConfig;

/**
 * Created by karansharma on 11/17/14.
 */
public class JobHandler {

   private String jobID;
   private boolean active = false;
   private Registry registry;
   private ScheduleManagerInterface scheduler;

   public JobHandler(String jobID)
   {
       this.jobID = jobID;
   }

   public void start(String[] fileIDs) throws Exception{
       this.active = true;
       registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
       scheduler = (ScheduleManagerInterface) registry.lookup("Scheduler");

       for(String file : fileIDs){
           /* Find all blocks of file */
           /* Try to run map operation for each block */

       }
   }

   public boolean getActive() {return this.active;}

}
