package Master;

import Config.InternalConfig;
import DFS.DFSBlock;
import Util.Host;

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

    public String selectReplica(String jobID, DFSBlock block){
        Set<Host> replicaHosts = block.getBlockHosts();
        //TODO: return node_id of replica with lowest wait
        return null;
    }
}
