package MapReduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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
        /* READ AND MAP */
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(inputFilePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String line;
        try {
            while ((line = br.readLine()) != null) {
                mapper.map(null,null,null);
            }


            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* SORT AND WRITE */
        String outFilePath = "";

        taskManager.checkReduce(jobID, outFilePath);
    }

}
