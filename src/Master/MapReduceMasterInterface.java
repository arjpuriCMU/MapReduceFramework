package Master;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MapReduceMasterInterface extends Remote {

	public void handshakeWithSlave(String participantID, String data_node_id) throws RemoteException;

}
