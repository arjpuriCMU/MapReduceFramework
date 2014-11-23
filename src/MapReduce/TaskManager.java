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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
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
    private int load;
    public ConcurrentHashMap<String, Integer> mapsLeft;
    public ConcurrentHashMap<String,HashSet<String>> mapOutputFiles;
    public ConcurrentHashMap<String,Mapper> mappers;
    public ConcurrentHashMap<String,Reducer> reducers;
    public Registry registry;
    public ExecutorService threadPool;
    public MapReduceMasterInterface master;
    public DataNodeInterface current_data_node;
    private ConcurrentHashMap<String,Integer> failureCounts;
    private int failureThreshold = 3;


    public TaskManager(String dataNodeID, int cores) throws RemoteException
    {
        this.dataNodeID = dataNodeID;
        this.cores = cores;
        mapsLeft = new ConcurrentHashMap<String,Integer>();
        mapOutputFiles = new ConcurrentHashMap<>();
        mappers = new ConcurrentHashMap<>();
        reducers = new ConcurrentHashMap<>();
        threadPool = Executors.newFixedThreadPool(cores);
        failureCounts = new ConcurrentHashMap<>();

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
		} catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    public synchronized void addJob(String jobID, Set<DFSBlock> dfsBlocks) throws RemoteException
    {
    	
        /* Get Mapper and Reducer Classes */
    	JavaCustomClassLoader map_loader = new JavaCustomClassLoader(master.getClassMap().get(jobID).getFirst());
    	Class<?> mapper_class =
    			map_loader.findClass(master.getClassNameMap().get(jobID).getFirst());
    	JavaCustomClassLoader reduce_loader = new JavaCustomClassLoader(master.getClassMap().get(jobID).getSecond());
    	Class<?> reducer_class = reduce_loader.findClass(master.getClassNameMap().get(jobID).getSecond());
    	Mapper mapper = null;
    	Reducer reducer = null;

        try {
            /* Instantiate and store mapper and reducers */
            mapper =  (Mapper) mapper_class.newInstance();
            reducer = (Reducer) reducer_class.newInstance();
            mappers.put(jobID,mapper);
            reducers.put(jobID,reducer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* Add map task to thread waiting list for each block */
        for(DFSBlock dfsBlock : dfsBlocks)
        {
            load++;
            threadPool.submit(new MapExecuter(this,jobID,mapper,dfsBlock.getHostBlockPath(dataNodeID)));
        }
        mapsLeft.put(jobID,dfsBlocks.size());
        mapOutputFiles.put(jobID,new HashSet<String>());
    }

    public void run(){}

    /* Called upon completion of each Map task. If there no more maps left
       then a reduce task is executed. Method is synchronized so reduce
       can only be launched once. Since the map thread is idling while the
       reduce is executed, the processor usage is still optimized.
     */
    public synchronized void checkReduce(String jobID, String outFileName)
    {
        /* Decrement mapsLeft value and store intermediate filename */
        load--;
        mapsLeft.put(jobID,mapsLeft.get(jobID) - 1);
        HashSet<String> outFiles =  mapOutputFiles.get(jobID);
        outFiles.add(outFileName);
        mapOutputFiles.put(jobID,outFiles);

        /* If last map was executed, execute reduce */
        if (mapsLeft.get(jobID) == 0)
        {
            ReduceExecuter reduceExecuter =
                    new ReduceExecuter(this, jobID, reducers.get(jobID), outFiles);
            reduceExecuter.run();
        }
        mapsLeft.put(jobID,-1);
    }

    public int taskLoad(){return load;}

    public int mapsLeft(String jobID) {
        return mapsLeft.get(jobID);
    }

    public String getDataNodeID(){return dataNodeID;}

    public void mapFailure(String jobID, String inputFilePath)
    {
        /* Update Failure Count for Job */

        if(!failureCounts.containsKey(jobID))
            failureCounts.put(jobID,1);
        else
        {
            int lastCount = failureCounts.put(jobID,failureCounts.get(jobID));

            /* Return if too many Failures */
            if(lastCount >= failureThreshold) {
                try {
					master.jobFailure(jobID,dataNodeID);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
                return;
            }
        }

        /* Resubmit Job */
        threadPool.submit(new MapExecuter(this,jobID,mappers.get(jobID),inputFilePath));

    }

    public void jobFailure(String jobID)
    {
        try {
			master.jobFailure(jobID,dataNodeID);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
    }

    public boolean jobCancelled(String jobID)
    {
        return failureCounts.containsKey(jobID) && (failureCounts.get(jobID) > failureThreshold);
    }

    /* Converts reduce output file to byte array and sends it to master */
    public void reduceComplete(String jobId,String filePath)
    {
        File file = new File(filePath);

        byte[] bytes = new byte[(int) file.length()];
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytes);
        }
        catch (Exception e) {
            jobFailure(jobId);
        }
        try {
			master.jobCompleted(jobId,dataNodeID,bytes);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
    }

    public void writeMROutput(ConcurrentHashMap<String,byte[]> output, String path)
    {
        /* Write each byte[] to a file on the given path */
        for (String nodeID : output.keySet())
        {
            String filePath = path + nodeID + "/Output.txt";
            System.out.println(filePath);
            FileOutputStream fos;
			try {
				fos = new FileOutputStream(filePath);
				fos.write(output.get(nodeID));
	            fos.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
            
        }
        System.out.println("Reduce output files have been written");

    }
}
