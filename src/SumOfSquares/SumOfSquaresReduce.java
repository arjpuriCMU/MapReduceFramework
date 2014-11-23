package SumOfSquares;

import java.util.ArrayList;

import IOFormat.MRCollector;
import MapReduce.Reducer;

public class SumOfSquaresReduce implements Reducer{

	@Override
	public void reduce(String key, ArrayList<String> value, MRCollector reducerCollector) {
        int sum = 0;
        for(String val : value)
        {
            sum += Integer.parseInt(val);
        }
        reducerCollector.addOutput(1,sum);

	}
	
}
