package MapReduce;

import IOFormat.MapperCollector;

public interface Mapper {
	
	public void map(Object key, Object value, MapperCollector mapperOutputCollector);
	public void sex();

}
