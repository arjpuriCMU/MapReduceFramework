package MapReduce;

        import IOFormat.MRCollector;
        import IOFormat.KeyValuePair;

        import java.io.*;
import java.util.*;

/**
 * Created by karansharma on 11/19/14.
 */
public class ReduceExecuter implements Runnable {

    private TaskManager taskManager;
    private String jobID;
    private Reducer reducer;
    private HashSet<String> filePaths;

    public ReduceExecuter(TaskManager taskManager, String jobID, Reducer reducer, HashSet<String> filePaths)
    {
        this.taskManager = taskManager;
        this.jobID = jobID;
        this.reducer = reducer;
        this.filePaths = filePaths;
    }

    public void run()
    {
        System.out.println("Reducing Locally");

        /* Merge Map Output to single Map */
        BufferedReader br = null;
        SortedMap<String,ArrayList<String>> mergedMapOutput = new TreeMap<String,ArrayList<String>>();
        String line;
        String tempPath = null;
        for(String filePath : filePaths)
        {
            /* Create Reader for File */
            try {
                br = new BufferedReader(new FileReader(filePath));
            } catch (FileNotFoundException e) {
                taskManager.jobFailure(jobID);
                return;            }

            try {
                while ((line = br.readLine()) != null) {
                    String[] kvp = line.split("=>");
                    ArrayList<String> vals = null;
                    if(mergedMapOutput.containsKey(kvp[0]))
                        vals = mergedMapOutput.get(kvp[0]);
                    else
                        vals = new ArrayList<>();
                    vals.add(kvp[1]);
                    mergedMapOutput.put(kvp[0],vals);
                }

                br.close();
            } catch (IOException e) {
                taskManager.jobFailure(jobID);
                return;
            }

            tempPath = filePath;
        }

        /* Collect Reduce Output */
        MRCollector collector = new MRCollector();
        for(String key : mergedMapOutput.keySet())
        {
            reducer.reduce(key,mergedMapOutput.get(key),collector);
        }

        /* Setup Reduce Output File */
        String outFile = null;
        if (tempPath != null)
            outFile = tempPath.replace(".txt","_reduceOutput.txt");

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(outFile, "UTF-8");
        } catch (Exception e) {
            taskManager.jobFailure(jobID);
            return;
        }

        /* Get SortedMap containing map output */
        ArrayList<KeyValuePair> reduceOutput = collector.getOutputCollection();

        /* Write each key value pair to file */
        for(KeyValuePair kvp : reduceOutput) {
            writer.println(kvp.toString());
        }
        writer.close();
        
        /* Notify Task Manager of outFilePath */
        taskManager.reduceComplete(jobID,outFile);


    }

}
