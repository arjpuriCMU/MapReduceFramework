package MapReduce;

import DFS.DFSBlock;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

/**
 * Created by karansharma on 11/18/14.
 */
public interface TaskManagerInterface extends Remote {
    public void addJob(String jobID, Set<DFSBlock> dfsBlocks) throws RemoteException;
    public int taskLoad() throws RemoteException;
    public String getDataNodeID() throws RemoteException;
}
