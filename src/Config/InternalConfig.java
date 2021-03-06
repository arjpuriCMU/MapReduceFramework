package Config;

public class InternalConfig {
	public static String NAME_NODE_ID = "DFSNameNode";
	public static String DFS_STORAGE_PATH  = "./dfs_storage/";
	public static int HEARTBEAT = 3;
	public static String HEALTH_MONITOR_ID = "health_monitor";
	public static int REGISTRY_PORT = 5001;
	public static String CONNECTION_MANAGER_ID = "connectionManager";
	public static String MAP_REDUCE_MASTER_ID = "Master_node";
	public static String REGISTRY_HOST;
	public static String JOB_TRACKER_ID = "jobTracker";
	public static String MASTER_HOSTNAME;
	
	public static String generateDataNodeId(String slave_id){
		return slave_id + "datanode";
	}
    public static String generateTaskManagerId(String hostname) {return hostname + "taskmanager";}
    
	
}
