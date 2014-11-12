package DFS;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import Config.ConfigSettings;

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
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
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
	
	private void initOnRegistry() {
		System.out.println(port + ", " + id + ", " + host);
		try {
			Registry registry = LocateRegistry.getRegistry(host,REGISTRY_PORT);
			registry.rebind(id, this);	
		} catch (RemoteException e) {
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
		try {
			registry = LocateRegistry.getRegistry(this.host, REGISTRY_PORT);
			health_monitor =  (HealthMonitor) registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
		} catch (RemoteException e1) {
			e1.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		while (active){
			try {
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
}
