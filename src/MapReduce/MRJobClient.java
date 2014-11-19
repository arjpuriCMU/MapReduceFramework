package MapReduce;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Config.InternalConfig;
import Util.ResultEnum;
import Util.Tuple;

public class MRJobClient {
	private String host_name;
	private int port;
	private Registry registry;
	private String job_id;
	private JobTrackerInterface job_tracker;
	public void runJob (MapReducerConfig config){
		try {
			registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST, InternalConfig.REGISTRY_PORT);
			job_tracker = (JobTrackerInterface) registry.lookup(InternalConfig.JOB_TRACKER_ID);
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		String mapper_name = config.getMapperClass().getName();
		String reducer_name = config.getMapperClass().getName();
		/* Example:  */
		Tuple<String,String> map_tuple = new Tuple<String,String>(mapper_name,mapper_name.replace('.', '/') + ".class");
		Tuple<String,String> red_tuple = new Tuple<String,String>(reducer_name, reducer_name.replace('.', '/'));
		try {
			ResultEnum result = job_tracker.submitJob(config,map_tuple,red_tuple);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
}
