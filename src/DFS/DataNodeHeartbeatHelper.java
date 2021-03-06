package DFS;

import Config.ConfigSettings;
import Config.InternalConfig;
import Util.Tuple;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNodeHeartbeatHelper extends UnicastRemoteObject implements Runnable, HeartbeatHelperInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3093276042895919449L;
	private int port;
	private String host;
	private boolean active;
	private String id;
	private String node_id;
	private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
	public DataNodeHeartbeatHelper(String node_id,String host, int port) throws RemoteException{
		this.host = host;
		this.port = port;
		active = true;
		this.node_id = node_id;
		id = getHeartbeatHelperID();
		initOnRegistry();
		
	}
	
	public String getHeartbeatHelperID(){
		return node_id+ "heartbeat";
	}
	
	/*Initialize DataNodeHeartbeatHelper on the data node registry */
	private void initOnRegistry() {
		Registry name_node_registry = null;
		try {
			/* Get the data node registry info */
			name_node_registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST,REGISTRY_PORT);
			DFSNameNodeInterface name_node = (DFSNameNodeInterface) name_node_registry.lookup(InternalConfig.NAME_NODE_ID);
			Tuple<String,Integer> data_node_registry_info = name_node.getDataNodeRegistryInfo().get(node_id);
			/*Bind the heartbeat helper to the data node registry */
			Registry data_node_registry = LocateRegistry.getRegistry(data_node_registry_info.getFirst(), data_node_registry_info.getSecond());
			data_node_registry.bind(id, this);
		} catch (RemoteException | NotBoundException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
	}

	public void setActive(boolean bool){
		active = bool;
	}

	@Override
	public void run() {
		Registry registry;
		HealthMonitor health_monitor = null;
		/* Get the health monitor from the master RMI registry to send heartbeat */
		try {
			registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, REGISTRY_PORT);
			health_monitor =  (HealthMonitor) registry.lookup(InternalConfig.HEALTH_MONITOR_ID);
		} catch (RemoteException e1) {
			e1.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
		while (active){
			try {
				/*Send heartbeat to health monitor at 'heartbeat_frequency' frequency */
				health_monitor.changeHeartbeat(node_id,20);
				Thread.sleep(ConfigSettings.heartbeat_frequency*1000);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		return;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}
