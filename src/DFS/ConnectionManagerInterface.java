package DFS;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ConnectionManagerInterface extends Remote {
	public void setActive(boolean bool) throws RemoteException;
	
}
