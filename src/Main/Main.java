package Main;
import java.io.IOException;
import java.net.InetAddress;

import Config.InternalConfig;
import DFS.DFSDataNode;
import DFS.DFSNameNode;
import MapReduce.TaskManager;
import Master.Master;


public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException{
		Master master = null;
		/*Creates the master node */
		if (args.length == 2){
			if (args[0].toLowerCase().equals("-m")){
				int input_port = Integer.parseInt(args[1]);
				master = new Master(input_port);
				String master_hostname = InetAddress.getLocalHost().getHostName();
				InternalConfig.MASTER_HOSTNAME = master_hostname;
				master.start();
			}
			else{
				System.out.println("Arguments were not recognized");
			}
		}
		/*Creates the datanode */
		else if (args.length == 4){ 
			if (args[0].toLowerCase().equals("-d")){
				System.out.println("Creating Data Node...");
				String node_id = args[1];
				int port = Integer.parseInt(args[2]);
				String host = args[3];
				InetAddress inet = InetAddress.getByName(host);
				DFSDataNode data_node = new DFSDataNode(node_id,inet,port);
				data_node.start();
				/* Construct and Start local Task Manager and bind it to the registry */
				TaskManager task_manager = new TaskManager(node_id,Runtime.getRuntime().availableProcessors());
		        (new Thread(task_manager)).start();
			}
			else{
				System.out.println("Arguments were not recognized");
			}
		}
		else{
			System.out.println("Arguments were not recognized");
		}
			
	}
}
