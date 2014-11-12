package DFS;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HealthMonitor extends Remote {
	public void addNode(String node) throws RemoteException;
	public void changeHeartbeats(Integer val) throws RemoteException;
	public void changeHeartbeat(String node_id, Integer val) throws RemoteException;
	public void printAllHealth() throws RemoteException;
}
