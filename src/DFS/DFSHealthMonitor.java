package DFS;

import java.net.InetAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;

public class DFSHealthMonitor extends UnicastRemoteObject implements Runnable, HealthMonitor {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static int HEARTBEAT_FREQUENCY = ConfigSettings.heartbeat_frequency;
	private ConcurrentHashMap<String, Integer> node_health_map;
	private InetAddress nameNode_host;
	private int port;
	private Set<String> node_ids;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;

	
	public DFSHealthMonitor(List<String> node_ids, InetAddress inetAddress, int port) throws RemoteException{
		nameNode_host = inetAddress; 
		this.setPort(port);
		this.node_ids = new HashSet<String>(node_ids);
		this.node_health_map = new ConcurrentHashMap<String,Integer>();
		addToRegistry();
		initializeHealth();
		
		
	}

	private void addToRegistry() {
		try {
			Registry registry = LocateRegistry.getRegistry(nameNode_host.getHostName(), REGISTRY_PORT);
			registry.bind(DFSConfig.HEALTH_MONITOR_ID, this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
		
	}

	private void initializeHealth() {
		for (String node_id : node_ids){
			node_health_map.put(node_id, 100);
		}
	}
	
	public void addNode(String node) throws RemoteException{
		node_ids.add(node);
		node_health_map.put(node, 200);
	}
	
	public void changeHeartbeats(Integer val) throws RemoteException{
		for (String node_id: node_ids){
			node_health_map.put(node_id, node_health_map.get(node_id)+val);
		}
	}

	public void changeHeartbeat(String node_id, Integer val) throws RemoteException{
			node_health_map.put(node_id, node_health_map.get(node_id)+val);
	}
	

	@Override
	public void run() {
		List<String> dead_node_ids;
		while (true){
			try{
				changeHeartbeats(-20);
				Thread.sleep(HEARTBEAT_FREQUENCY*1000);
				if ((dead_node_ids = findInactiveNodes()) != null){
					removeInactiveNodes(dead_node_ids);
				}
			} catch (InterruptedException e) {
				System.out.println("Health monitor interrupt");
				return;
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			finally{
				
			}
		}
	}

	private void removeInactiveNodes(List<String> dead_node_ids) {
		Registry registry = null;
		try {
			registry = LocateRegistry.getRegistry(nameNode_host.getHostName(), REGISTRY_PORT);
			DFSNameNodeInterface name_node = (DFSNameNodeInterface) registry.lookup(DFSConfig.NAME_NODE_ID);
			for (int i = 0; i < dead_node_ids.size(); i++){
				name_node.changeActiveStatus(dead_node_ids.get(i));
				node_ids.remove(dead_node_ids.get(i));
				name_node.moveBlocksFromInactive();
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
	}

	private List<String> findInactiveNodes() {
		List<String> dead_nodes = new ArrayList<String>();
		for (String node_id: node_ids){
			if (node_health_map.get(node_id) <=0){
				dead_nodes.add(node_id);
			}
		}
		
		if (dead_nodes.size() !=0){
			return dead_nodes;
		}
		
		return null;
		
	}

	public void printAllHealth() throws RemoteException {
		for (String node_id: this.node_health_map.keySet()){
			System.out.println(node_id + ": " + this.node_health_map.get(node_id));
		}
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
