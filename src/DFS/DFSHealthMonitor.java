package DFS;

import java.net.InetAddress;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;

public class DFSHealthMonitor implements Runnable, Remote {

	
	private static int HEARTBEAT_FREQUENCY = ConfigSettings.heartbeat_frequency;
	private ConcurrentHashMap<String, Integer> node_health_map;
	private InetAddress nameNode_host;
	private int port;
	private List<String> node_ids;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;

	
	public DFSHealthMonitor(List<String> node_ids, InetAddress inetAddress, int port) {
		nameNode_host = inetAddress; 
		this.port = port;
		this.node_ids = node_ids;
		addToRegistry();
		initializeHealth();
		
		
	}

	private void addToRegistry() {
		Remote current_stub;
		try {
			current_stub =  UnicastRemoteObject.exportObject(this,REGISTRY_PORT);
			Registry registry = LocateRegistry.getRegistry(nameNode_host.getHostName(), REGISTRY_PORT);
			registry.bind(DFSConfig.HEALTH_MONITOR_ID, current_stub);
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
	
	public void addNode(String node){
		node_ids.add(node);
		node_health_map.put(node, 100);
	}
	
	public void changeHeartbeats(Integer val){
		for (String node_id: node_ids){
			node_health_map.put(node_id, node_health_map.get(node_id)+val);
		}
	}

	public void dataNodeHeartbeat(String dataNodeId){
		node_health_map.put(dataNodeId,node_health_map.get(dataNodeId)+20);
	}

	@Override
	public void run() {
		List<String> dead_node_ids;
		while (true){
			try{
				changeHeartbeats(-20);
				Thread.sleep(HEARTBEAT_FREQUENCY*1000);
				if ((dead_node_ids = findDeadNodes()) != null){
					removeDeadNodes(dead_node_ids);
				}
			} catch (InterruptedException e) {
				System.out.println("Health monitor interrupt");
				return;
			}
			finally{
				
			}
		}
	}

	private void removeDeadNodes(List<String> dead_node_ids) {
		Registry registry = null;
		try {
			registry = LocateRegistry.getRegistry(nameNode_host.getHostName(), REGISTRY_PORT);
			for (int i = 0; i < dead_node_ids.size(); i++){
				DFSDataNode temp_node = (DFSDataNode) registry.lookup(dead_node_ids.get(i));
				DataNodeHeartbeatHelper heartbeat_helper  = (DataNodeHeartbeatHelper) registry.lookup(temp_node.getHeartbeatHelperID());
				temp_node.setActive(false);
				heartbeat_helper.setActive(false);
				node_ids.remove(dead_node_ids.get(i));
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
			
			
		
	}

	private List<String> findDeadNodes() {
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

	public void printAllHealth() {
		for (String node_id: this.node_health_map.keySet()){
			System.out.println(node_id + ": " + this.node_health_map.get(node_id));
		}
	}

}
