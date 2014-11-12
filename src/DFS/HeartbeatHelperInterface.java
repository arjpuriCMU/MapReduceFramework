package DFS;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HeartbeatHelperInterface extends Remote{
	public String getHeartbeatHelperID() throws RemoteException;
	public void setActive(boolean bool) throws RemoteException;
	
}
