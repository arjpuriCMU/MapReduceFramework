package MapReduce;

        import IOFormat.KeyValuePair;
        import IOFormat.MapperCollector;
        import IOFormat.ReducerCollector;

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
        /* Merge Map Output to single Map */
        BufferedReader br = null;
        SortedMap<String,ArrayList<String>> mergedMapOutput = new TreeMap<String,ArrayList<String>>();
        String line;

        for(String filePath : filePaths)
        {
            /* Create Reader for File */
            try {
                br = new BufferedReader(new FileReader(filePath));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            try {
                while ((line = br.readLine()) != null) {
                    String[] kvp = line.split("=>");
                    ArrayList<String> vals = null;
                    if(mergedMapOutput.containsKey(kvp[1]))
                        vals = mergedMapOutput.get(kvp[0]);
                    else
                        vals = new ArrayList<>();
                    vals.add(kvp[1]);
                    mergedMapOutput.put(kvp[0],vals);
                }

                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        /* Collect Reduce Output */
        ReducerCollector collector = new ReducerCollector();
        for(String key : mergedMapOutput.keySet())
        {
            reducer.reduce(key,mergedMapOutput.get(key),collector);
        }

        /* Setup Reduce Output File */
        String outFilePath = "";
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(outFilePath, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        /* Get SortedMap containing map output */
        ArrayList<KeyValuePair> reduceOutput = collector.getReducerOutputCollection();

        /* Write each key value pair to file */
        if(writer == null) {
            //TODO: FAILURE HANDLING
            return;
        }

        for(KeyValuePair kvp : reduceOutput) {
            writer.println(kvp.toString());
        }
        writer.close();

    }

}
