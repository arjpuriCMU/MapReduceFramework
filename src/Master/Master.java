package Master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import Config.InternalConfig;
import DFS.DFSNameNode;
import Util.Host;

public class Master extends UnicastRemoteObject implements MapReduceMasterInterface {
	private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
	private int port;
	private Host name_node_host;
	private Registry main_registry;
	private DFSNameNode name_node;
	

	public Master(int port) throws RemoteException{
		this.port = port;
		try {
			InternalConfig.REGISTRY_HOST = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		System.out.println("NameNode being initiated.");
		name_node = new DFSNameNode(port);
		name_node.start();
		try {
			main_registry = LocateRegistry.createRegistry(REGISTRY_PORT);
			main_registry.bind(InternalConfig.MAP_REDUCE_MASTER_ID, this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} 
		name_node.initRegistry();
		setName_node_host(new Host(name_node.getHost().getHostName(),port));
	}

	public Host getName_node_host() {
		return name_node_host;
	}


	public void setName_node_host(Host name_node_host) {
		this.name_node_host = name_node_host;
	}
	
	
	
}
