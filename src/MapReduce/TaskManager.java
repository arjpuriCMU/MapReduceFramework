package MapReduce;

import DFS.DFSBlock;
import Util.Tuple;

import java.io.File;
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
public class TaskManager implements Runnable,TaskManagerInterface{

    public String dataNodeID;
    public int cores;
    public int load;
    public ConcurrentHashMap<String, Integer> mapsLeft;
    public ConcurrentHashMap<String,HashSet<String>> mapOutputFiles;
    public ConcurrentHashMap<String,Mapper> mappers;
    public ConcurrentHashMap<String,Reducer> reducers;
    ExecutorService threadPool;

    public TaskManager(String dataNodeID, int cores){
        this.dataNodeID = dataNodeID;
        this.cores = cores;
        mapsLeft = new ConcurrentHashMap<String,Integer>();
        mapOutputFiles = new ConcurrentHashMap<>();
        mappers = new ConcurrentHashMap<>();
        reducers = new ConcurrentHashMap<>();
        threadPool = Executors.newFixedThreadPool(cores);


    }

    public void addJob(String jobID, Set<DFSBlock> dfsBlocks, Class mapperClass, Class reducerClass)
    {
        /* TODO: Get Mapper and Reducer Classes */
        Mapper mapper = null;
        Reducer reducer = null;
        try {
            mapper = (Mapper) mapperClass.newInstance();
            reducer = (Reducer) reducerClass.newInstance();
            mappers.put(jobID,(Mapper) mapperClass.newInstance());
            reducers.put(jobID,(Reducer) reducerClass.newInstance());
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
        HashSet outFiles =  mapOutputFiles.get(jobID);
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
