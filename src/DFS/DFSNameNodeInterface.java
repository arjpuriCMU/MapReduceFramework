package DFS;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Util.Host;

public interface DFSNameNodeInterface extends Remote {
	public ConcurrentHashMap<String,Boolean> getIdActiveMap() throws RemoteException;
	public ConcurrentHashMap<String,Host> getIdHostMap() throws RemoteException;
	public void start() throws RemoteException;
	public void partitionAndDistributeFiles() throws RemoteException;
	public void listFiles() throws RemoteException;
	public ServerSocket getServerSocket() throws RemoteException;
	public void startHealthChecker(List<String> node_ids) throws RemoteException;
	public void moveBlocksFromInactive() throws RemoteException;
	public ConcurrentHashMap<String, List<DFSBlock>> getIdBlockMap() throws RemoteException;
	public List<String> getActiveNodes() throws RemoteException;
	public InetAddress getHost() throws RemoteException;
	public void changeActiveStatus(String node_id) throws RemoteException;
	public void bindFileFromByteArray(byte[] byte_array, String string) throws RemoteException;

}
