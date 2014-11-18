package MapReduce;

public interface Mapper {
	
	public void map(Object key, Object value, MapperCollector mapperOutputCollector);

}
