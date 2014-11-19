package MapReduce;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import Config.ConfigSettings;
import Config.InternalConfig;
import DFS.DFSNameNodeInterface;
import Util.ResultEnum;
import Util.State;
import Util.Tuple;

/* A single JobTracker is started on the Master. Coordinates all the slaves, and dispatches jobs */
public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {
	private static volatile int central_job_id;
	private ConcurrentHashMap<Integer,State> jobID_state;
	private DFSNameNodeInterface name_node;
	private Registry registry;
	public JobTracker() throws RemoteException{
		central_job_id = 0;
		jobID_state = new ConcurrentHashMap<Integer,State>();
		initOnRegistry();
	}
	
	private void initOnRegistry() throws RemoteException {
		registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
		try {
			registry.bind(InternalConfig.JOB_TRACKER_ID, this);
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
	}

	public ResultEnum submitJob(MapReducerConfig config, Tuple<String,String> map_tuple, 
			Tuple<String,String> reduce_tuple){
		/*Check if the file exists on the DFS */
		try {
			name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
			if (!name_node.fileExists(config.getInput_file())){
				return ResultEnum.InputFileAbsent;
			}
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			
		}
		
		/*Create a new JobID */
		int curr_job_id = central_job_id;
		jobID_state.put(central_job_id, State.Running);
		synchronized (Integer.valueOf(central_job_id)){
			central_job_id++;
		}
		
		/*Get the node to work on and the blocks to work from the job Scheduler */
		
		
		
		/*copy the Mapper and the Reducer to the local directory */
		establishJobLocally(map_tuple,reduce_tuple, curr_job_id);
		
		
	}

	private void establishJobLocally(Tuple<String, String> map_tuple,
			Tuple<String, String> reduce_tuple, int curr_job_id) {
		String[] mapper_name = (map_tuple.getFirst().split("\\."));
		String[] reducer_name = (reduce_tuple.getFirst().split("\\."));
		String map_path = ConfigSettings.UPLOAD_PATH + mapper_name[mapper_name.length-1] + "-" + curr_job_id + ".class";
		String reduce_path = ConfigSettings.UPLOAD_PATH + reducer_name[reducer_name.length-1] + "-" + curr_job_id + ".class";
		
		FileFunctions.transferBinary(map_tuple.getSecond().getBytes(),map_path);
		
		
	}
	
	
}
