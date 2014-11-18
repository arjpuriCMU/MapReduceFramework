package DFS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Util.Host;

public interface DFSNameNodeInterface extends Remote {
	public ConcurrentHashMap<String,Boolean> getIdActiveMap() throws RemoteException;
	public ConcurrentHashMap<String,Host> getIdHostMap() throws RemoteException;
	public void start() throws RemoteException;
	public void partitionAndDistributeFiles(List<DFSFile> list) throws RemoteException, IOException;
	public void listFiles() throws RemoteException;
	public ServerSocket getServerSocket() throws RemoteException;
	public void startHealthChecker(List<String> node_ids) throws RemoteException;
	public void moveBlocksFromInactive() throws RemoteException;
	public ConcurrentHashMap<String, List<DFSBlock>> getIdBlockMap() throws RemoteException;
	public List<String> getActiveNodes() throws RemoteException;
	public InetAddress getHost() throws RemoteException;
	public void changeActiveStatus(String node_id) throws RemoteException;
	public void bindFileFromByteArray(String string, byte[] byte_array, String job_id, String mapreducer_id) throws RemoteException;
	public Set<String> flushFilesToDataNodes(String map_reducer_id) throws RemoteException;

}
