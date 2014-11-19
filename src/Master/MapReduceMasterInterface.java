package Master;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import MapReduce.MapReducerConfig;
import Util.Tuple;

public interface MapReduceMasterInterface extends Remote {

	public void handshakeWithSlave(String participantID, String data_node_id) throws RemoteException;
    public String createJob(String participantID,MapReducerConfig config, byte[] map_class_byte_array, byte[] reduce_class_byte_array, Tuple<String, String> map_tuple, Tuple<String, String> red_tuple) throws RemoteException;
    public void startJob(String jobID, Set<String> file_ids, MapReducerConfig config) throws RemoteException, Exception;
    public ConcurrentHashMap<String,Tuple<byte[],byte[]>> getClassMap() throws RemoteException;
    public ConcurrentHashMap<String,Tuple<String,String>> getClassNameMap() throws RemoteException;

}
