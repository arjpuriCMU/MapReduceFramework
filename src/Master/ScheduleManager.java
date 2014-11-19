package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import MapReduce.TaskManager;
import MapReduce.TaskManagerInterface;
import Util.Host;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Set;

/**
 * Created by karansharma on 11/17/14.
 */
public class ScheduleManager implements ScheduleManagerInterface, Runnable{

    //Queue<DFSBlock>
    Registry registry;
    public ScheduleManager()
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
    public String selectReplica(String jobID, DFSBlock block){
        Set<Host> replicaHosts = block.getBlockHosts();
        int minLoad = -1;
        String minHostName = "";
        for(Host replicaHost : replicaHosts)
        {
            TaskManagerInterface taskManager = null;
            try {
                taskManager =
                    (TaskManagerInterface) registry.lookup(InternalConfig.generateTaskManagerId(replicaHost.hostname));
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
}
