package MapReduce;

import IOFormat.KeyValuePair;
import IOFormat.MRCollector;

import java.io.*;
import java.util.*;

/**
 * Created by karansharma on 11/19/14.
 */
public class MapExecuter implements Runnable {

    private TaskManager taskManager;
    private String jobID;
    private Mapper mapper;
    private String inputFilePath;

    public MapExecuter(TaskManager taskManager, String jobID, Mapper mapper, String inputFilePath)
    {
        this.taskManager = taskManager;
        this.jobID = jobID;
        this.mapper = mapper;
        this.inputFilePath = inputFilePath;
    }

    public void run()
    {
        System.out.println("Mapping File: " + inputFilePath);

        /* Check if Job Cancelled */
        if(taskManager.jobCancelled(jobID))
        {
            System.out.println("Map Failed");
            return;
        }

        /* READ AND MAP */

        /* open reader */
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(inputFilePath));
        } catch (FileNotFoundException e) {
            taskManager.mapFailure(jobID,inputFilePath);
            return;
        }

        /* Create collecter and execute user map on each line of input file */
        MRCollector collector = new MRCollector();
        String line;
        try {
            while ((line = br.readLine()) != null) {
                mapper.map(line,collector);
            }

            br.close();
        } catch (IOException e) {
            taskManager.mapFailure(jobID, inputFilePath);
            return;
        }

        /* Setup Intermediate Map Output File */
        String outFilePath = inputFilePath.replace(".txt","out.txt");
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(outFilePath, "UTF-8");
        } catch (Exception e) {
            taskManager.mapFailure(jobID, inputFilePath);
            return;
        }

        /* Get SortedMap containing map output */
        ArrayList<KeyValuePair> mapOutput = collector.getOutputCollection();
        Collections.sort(mapOutput);

        /* Write each key value pair to file */
        for(KeyValuePair kvp : mapOutput) {
            writer.println(kvp.toString());
        }
        writer.close();

        /* Notify task manager of completion and perform reduce if necessary */
        taskManager.checkReduce(jobID, outFilePath);
    }

}
