package MapReduce;

import Config.InternalConfig;
import DFS.DFSBlock;
import DFS.DFSDataNode;
import DFS.DFSNameNodeInterface;
import DFS.DataNodeInterface;
import Master.MapReduceMasterInterface;
import Master.Master;
import Util.JavaCustomClassLoader;
import Util.Tuple;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by karansharma on 11/17/14.
 */
public class TaskManager extends UnicastRemoteObject implements Runnable,TaskManagerInterface{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String dataNodeID;
    public int cores;
    public int load;
    public ConcurrentHashMap<String, Integer> mapsLeft;
    public ConcurrentHashMap<String,HashSet<String>> mapOutputFiles;
    public ConcurrentHashMap<String,Mapper> mappers;
    public ConcurrentHashMap<String,Reducer> reducers;
    public Registry registry;
    ExecutorService threadPool;
    public MapReduceMasterInterface master;
    public DataNodeInterface current_data_node;

    public TaskManager(String dataNodeID, int cores) throws RemoteException{
        this.dataNodeID = dataNodeID;
        this.cores = cores;
        mapsLeft = new ConcurrentHashMap<String,Integer>();
        mapOutputFiles = new ConcurrentHashMap<>();
        mappers = new ConcurrentHashMap<>();
        reducers = new ConcurrentHashMap<>();
        threadPool = Executors.newFixedThreadPool(cores);
        /* Master registry */
        registry = LocateRegistry.getRegistry(InternalConfig.REGISTRY_HOST,InternalConfig.REGISTRY_PORT);
        try {
        	master = (MapReduceMasterInterface) registry.lookup(InternalConfig.MAP_REDUCE_MASTER_ID);
        	
        	/*Get the name node from the central registry */
        	DFSNameNodeInterface name_node = (DFSNameNodeInterface) registry.lookup(InternalConfig.NAME_NODE_ID);
        	/* Get the corresponding data node's registry info */
        	String registry_host = name_node.getDataNodeRegistryInfo().get(dataNodeID).getFirst();
			int registry_port = name_node.getDataNodeRegistryInfo().get(dataNodeID).getSecond();
			/*Get the data node registry */
			Registry data_node_registry = LocateRegistry.getRegistry(registry_host,registry_port);
			current_data_node = (DataNodeInterface) data_node_registry.lookup(dataNodeID);
			/*bind task manager to data node's registry */
			data_node_registry.bind(InternalConfig.generateTaskManagerId(current_data_node.getHostName()), this);
		} catch (NotBoundException e1) {
			e1.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
    }

    public void addJob(String jobID, Set<DFSBlock> dfsBlocks) throws RemoteException
    {
    	
        /*Get Mapper and Reducer Classes */
    	JavaCustomClassLoader map_loader = new JavaCustomClassLoader(master.getClassMap().get(jobID).getFirst());
    	Class<?> mapper_class =
    			map_loader.findClass(master.getClassNameMap().get(jobID).getFirst());
    	JavaCustomClassLoader reduce_loader = new JavaCustomClassLoader(master.getClassMap().get(jobID).getSecond());
    	Class<?> reducer_class = reduce_loader.findClass(master.getClassNameMap().get(jobID).getSecond());
    	Mapper mapper = null;
    	Reducer reducer = null;
        try {
            mapper =  (Mapper) mapper_class.newInstance();
            reducer = (Reducer) reducer_class.newInstance();
            mapper.sex(); /* testing only dont judge */
            reducer.poop();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        for(DFSBlock dfsBlock : dfsBlocks)
        {
            load++;
            threadPool.submit(new MapExecuter(this,jobID,mapper,dfsBlock.getHostBlockPath(dataNodeID)));
        }
        mapsLeft.put(jobID,dfsBlocks.size());
    }

    public void run(){}

    public void checkReduce(String jobID, String outFileName)
    {
        //TODO: lock around load
        load--;
        mapsLeft.put(jobID,mapsLeft.get(jobID) - 1);
        HashSet<String> outFiles =  mapOutputFiles.get(jobID);
        outFiles.add(outFileName);
        mapOutputFiles.put(jobID,outFiles);

        /* If last map was executed, execute reduce */
        if (mapsLeft.get(jobID) == 0)
        {
            ReduceExecuter reduceExecuter =
                    new ReduceExecuter(this,jobID, reducers.get(jobID), outFiles);
            reduceExecuter.run();
        }
    }

    public int taskLoad(){return load;}

    public String getDataNodeID(){return dataNodeID;}
}
