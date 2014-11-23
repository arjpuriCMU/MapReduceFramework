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
    public void checkReduce(String jobID, String outFileName) throws RemoteException;
    public int taskLoad() throws RemoteException;
    public String getDataNodeID() throws RemoteException;
    public void mapFailure(String jobID, String inputFilePath) throws RemoteException;
    public void jobFailure(String jobID) throws RemoteException;
    public boolean jobCancelled(String jobID) throws RemoteException;
    public void reduceComplete(String jobId,String filePath) throws RemoteException;
    public int mapsLeft(String jobID) throws RemoteException;
    public void writeMROutput(ConcurrentHashMap<String,byte[]> output,String jobID, String path) throws RemoteException;
}
