package SumOfSquares;

import IOFormat.MRCollector;
import MapReduce.Mapper;

public class SumOfSquaresMap implements Mapper {

	@Override
	public void map(String line, MRCollector mapperOutputCollector) {
        String[] strInts = line.split(" ");
        for(String strInt : strInts)
        {
            int x = Integer.parseInt(strInt);
            mapperOutputCollector.addOutput(1,x*x);
        }
	}
	
}
