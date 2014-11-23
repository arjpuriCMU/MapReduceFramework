package MapReduce;

import java.io.Serializable;



public class MapReducerConfig implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4582943943235174948L;
	private Class<?> mapper_class;
	private Class<?> reducer_class;
	private String input_file;
	private String output_file_path;
	
	public void setMapperClass(Class<?> map_class) {
		this.mapper_class = map_class;
	}
	
	public Class<?> getMapperClass(){
		return mapper_class;
	}
	
	public void setReducerClass(Class<?> reduce_class){
		this.reducer_class = reduce_class;
	}
	
	public Class<?> getReducerClass(){
		return this.reducer_class;
	}

	public void setInputFile(String string) {
		this.setInput_file(string);
	}

	public String getInput_file() {
		return input_file;
	}

	public void setInput_file(String input_file) {
		this.input_file = input_file;
	}

	public String getOutputFilePath() {
		return output_file_path;
	}

	public void setOutputFilePath(String output_file) {
		this.output_file_path = output_file;
	}
	
	
}
