package Main;
import java.io.IOException;
import java.net.InetAddress;

import DFS.DFSDataNode;
import DFS.DFSNameNode;


public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException{
		
		DFSNameNode master = null;
		if (args.length == 2){
			if (args[0].toLowerCase().equals("-n")) {
				System.out.println("NameNode being initiated.");
				int input_port = Integer.parseInt(args[1]);
				master = new DFSNameNode(input_port);
				master.start();
			}
		}
		else if (args.length == 4){
			if (args[0].toLowerCase().equals("-d")){
				System.out.println("Creating Data Node...");
				int port = Integer.parseInt(args[2]);
				String node_id = args[1];
				String host = args[3];
				InetAddress inet = InetAddress.getByName(host);
				DFSDataNode data_node = new DFSDataNode(node_id,inet,port);
				data_node.sendHeartbeatMessage(host, port);
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
