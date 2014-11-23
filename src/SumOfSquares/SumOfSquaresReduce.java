package SumOfSquares;

import java.util.ArrayList;

import IOFormat.MRCollector;
import MapReduce.Reducer;

public class SumOfSquaresReduce implements Reducer{

	@Override
	public void reduce(String key, ArrayList<String> value,
			MRCollector reducerCollector) {

	}
	
}
