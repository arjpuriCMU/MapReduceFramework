package MapReduce;

        import java.io.BufferedReader;
        import java.io.FileNotFoundException;
        import java.io.FileReader;
        import java.io.IOException;
        import java.util.HashSet;

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
        /* READ AND MAP */


    }

}
