package MapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Util.ResultEnum;
import Util.Tuple;

public interface JobTrackerInterface extends Remote {

	ResultEnum submitJob(MapReducerConfig config,
			Tuple<String, String> map_tuple, Tuple<String, String> red_tuple) throws RemoteException;

}
