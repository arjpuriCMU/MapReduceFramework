package DFS;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Config.InternalConfig;
import Messages.Handshake;
import Util.Host;

public class DFSConnectionManager extends UnicastRemoteObject implements Runnable, ConnectionManagerInterface {
	private static final long serialVersionUID = 1L;
	private DFSNameNode master_name_node;
    private ConcurrentHashMap<String, Socket> nodeId_socket_map;
    private List<String> node_ids; /*Stores all the connected nodes */
    private final int REGISTRY_PORT = InternalConfig.REGISTRY_PORT;
    private boolean active = true;

    public DFSConnectionManager(DFSNameNode dfsNameNode) throws RemoteException {
        master_name_node = dfsNameNode;
        this.nodeId_socket_map = new ConcurrentHashMap<String, Socket>();
        node_ids = new ArrayList<String>();
        initOnRegistry();
    }

    /*Adds the connection manager to the Master's RMI Registry for communication to NameNode etc. */
    private void initOnRegistry() {
        try {
            Registry registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, REGISTRY_PORT);
            registry.bind(InternalConfig.CONNECTION_MANAGER_ID, this);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }

    }


    public void setActive(boolean bool) {
        this.active = bool;
    }

    public void run() {
        System.out.println("Connection Manager started...");
        ServerSocket server_socket = master_name_node.getServerSocket();
        Socket data_node_socket = null;
        ObjectInputStream input_stream = null;
        int i = 0;

        /*Start Health Monitor */
        master_name_node.startHealthChecker(node_ids);

       
        while (active) {
            try {
                /*Accept DataNode connection and cast input message as Handshake
                 *Uses the NameNode's server socket to establish handshake
                 */
                data_node_socket = server_socket.accept();
                input_stream = new ObjectInputStream(data_node_socket.getInputStream());
                Handshake handshake_msg = (Handshake) input_stream.readObject();

                /*Cache socket of dataNode */
                this.nodeId_socket_map.put(handshake_msg.getNodeId(), data_node_socket);

                /*
                    dataNode has connected to master Node before, so master should
                    send back its files as a failure precaution
                 */
                if (master_name_node.getNodeIds().contains(handshake_msg.getNodeId())) {
                    /*put the connection manager to sleep to avoid race condition to data node */
                	Thread.sleep(1000);
                    master_name_node.returnFilesToNode(handshake_msg.getNodeId());
                }

                /* First time dataNode is connecting to masterNode */
                else {
                    master_name_node.getIdBlockMap().put(handshake_msg.getNodeId(), new ArrayList<DFSBlock>());

                    /* Store dataNode information on nameNode */
                    master_name_node.node_ids.add(handshake_msg.getNodeId());
                    master_name_node.getIdHostMap().put(handshake_msg.getNodeId()
                            , new Host(handshake_msg.getDataNodeHost()
                            , data_node_socket.getPort()));
                    i++;
                    Registry registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST
                            , REGISTRY_PORT);
                    /*Stores node as active */
                    master_name_node.getIdActiveMap().put(handshake_msg.getNodeId(), true);
                    
                    /*Add the datanode to the health monitor so it can monitor currently added data node's status */
                    HealthMonitor health_monitor = (HealthMonitor) registry.lookup(InternalConfig.HEALTH_MONITOR_ID);
                    health_monitor.addNode(handshake_msg.getNodeId());
                    System.out.println("DataNode Id: " + handshake_msg.getNodeId() + " has started..");
                    System.out.print("Master -> ");
                }
                data_node_socket.close();
                input_stream.close();

            }catch (SocketException e){
            	
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        }

    }
}
