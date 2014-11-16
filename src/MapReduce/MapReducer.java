package MapReduce;

import DFS.DFSDataNode;
import Util.Host;

import java.io.File;
import java.net.InetAddress;

/**
 * Created by karansharma on 11/13/14.
 *
 * MapReducer is the object that handles interactions with the master map
 * reducer node. So if a user wants to map reduce a job, it first creates a
 * MapReducer object which will have a dataNode utilized for other active
 * jobs in the system. When the user wants to run a job on this MapReducer,
 * a class extending the MapReduceInterface must be created and then sent
 * to runJob along with the input files of the job.
 */
public class MapReducer {

    private DFSDataNode data_node;

    /* Constructor that connects to master node of MapReduce system.
       ParticipantID specifies the name of this user for future use.
     */
    public MapReducer(String participantID, Host master) throws Exception {
        try {

            InetAddress inet = InetAddress.getByName(master.hostname);
            data_node = new DFSDataNode(participantID, inet, master.port);
            data_node.start();
        }
        catch (Exception e)
        {
            throw new Exception("Failed during Data Node Construction");
        }
    }

    public void runJob(MapReduceInterface jobClass, File[] files){

    }
}
