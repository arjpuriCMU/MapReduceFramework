package MapReduce;

import DFS.DFSBlock;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karansharma on 11/18/14.
 */
public interface TaskManagerInterface extends Remote {
    public void addJob(String jobID, Set<DFSBlock> dfsBlocks) throws RemoteException;
    public int taskLoad() throws RemoteException;
    public String getDataNodeID() throws RemoteException;
    public int mapsLeft(String jobID);
    public void writeMROutput(ConcurrentHashMap<String,byte[]> output, String path) throws IOException;
}
