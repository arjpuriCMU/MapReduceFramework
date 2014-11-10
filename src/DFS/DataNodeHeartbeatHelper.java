package DFS;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import Config.ConfigSettings;

public class DataNodeHeartbeatHelper implements Runnable, Remote {
	private int port;
	private String host;
	private boolean active;
	private String id;
	private final int REGISTRY_PORT = DFSConfig.REGISTRY_PORT;
	public DataNodeHeartbeatHelper(String node_id,String host, int port){
		this.host = host;
		this.port = port;
		active = true;
		initOnRegistry();
		id = getHeartbeatHelperID();
	}
	
	public String getHeartbeatHelperID(){
		return id+ "heartbeat";
	}
	
	private void initOnRegistry() {
		DataNodeHeartbeatHelper current_stub;
		try {
			current_stub = (DataNodeHeartbeatHelper) UnicastRemoteObject.exportObject(this,port);
			Registry registry = LocateRegistry.getRegistry(host,port);
			registry.bind(id, current_stub);	
		} catch (RemoteException e) {
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
		while (active){
			Registry registry;
			try {
				registry = LocateRegistry.getRegistry(this.host, REGISTRY_PORT);
				DFSHealthMonitor health_monitor =  (DFSHealthMonitor) registry.lookup(DFSConfig.HEALTH_MONITOR_ID);
				health_monitor.changeHeartbeats(20);
				Thread.sleep(ConfigSettings.heartbeat_frequency*1000);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
}
