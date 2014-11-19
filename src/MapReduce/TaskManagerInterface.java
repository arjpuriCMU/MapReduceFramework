package MapReduce;

import DFS.DFSBlock;

import java.rmi.Remote;
import java.util.Set;

/**
 * Created by karansharma on 11/18/14.
 */
public interface TaskManagerInterface extends Remote {
    public void addJob(String jobID, Set<DFSBlock> dfsBlocks, String JarFileName);
    public void launchThread(int index);
    public int taskLoad();
    public String getDataNodeID();
}
