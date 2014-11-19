package MapReduce;

import DFS.DFSBlock;
import Util.Tuple;

import java.io.File;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karansharma on 11/17/14.
 */
public class TaskManager implements Runnable,TaskManagerInterface{

    public String dataNodeID;
    public int cores;
    public LinkedList<Tuple<String,DFSBlock>> taskList;
    public ConcurrentHashMap<String, Integer> mapsLeft;
    public ConcurrentHashMap<String,Set<File>> mapOutputFiles;
    public Thread[] threads;

    public TaskManager(String dataNodeID, int cores){
        this.dataNodeID = dataNodeID;
        this.cores = cores;
        taskList = new LinkedList<>();
        mapsLeft = new ConcurrentHashMap<String,Integer>();
        mapOutputFiles = new ConcurrentHashMap<>();
        threads = new Thread[cores];
    }
    
    public void addJob(String jobID, Set<DFSBlock> dfsBlocks, String JarFileName){
        for(DFSBlock dfsBlock : dfsBlocks)
        {
            taskList.add(new Tuple(jobID, dfsBlock));
        }
        mapsLeft.put(jobID,dfsBlocks.size());
    }

    public void run(){
        for(int i = 0; i < cores; i++)
        {
            if(taskList.size() > 0)
            {
                Tuple<String,DFSBlock> task = taskList.removeFirst();

            }

        }

    }

    public void launchThread(int index){
        if(taskList.size() > 0)
        {
            Tuple<String,DFSBlock> task = taskList.removeFirst();

        }
        return;

    }

    public int taskLoad(){return taskList.size();}

    public String getDataNodeID(){return dataNodeID;}
}
