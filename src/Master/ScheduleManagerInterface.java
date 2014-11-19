package Master;

import DFS.DFSBlock;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by karansharma on 11/17/14.
 */
public interface ScheduleManagerInterface extends Remote{
    public String selectReplica(String jobID, DFSBlock block) throws RemoteException;
}
