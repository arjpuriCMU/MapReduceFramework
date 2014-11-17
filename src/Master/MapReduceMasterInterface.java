package Master;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MapReduceMasterInterface extends Remote {

	public void handshakeWithSlave(String participantID, String data_node_id) throws RemoteException;
    public String createJob(String participantID,String JarFileName);
    public void startJob(String jobID, String[] fileIDs) throws Exception;

}
