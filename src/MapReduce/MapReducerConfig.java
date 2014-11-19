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
	private String output_file;
	private Class<?> input_format;
	private Class<?> output_format;
	
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

	public void setOutputFile(String string) {
		this.setOutput_file(string);
	}

	public void setInputFormat(Class<?> input_format_class) {
		this.input_format = input_format_class;
	}

	public void setOutputFromat(Class<?> output_format_class) {
		this.output_format = output_format_class;
	}
	
	public Class<?> getInputFormat(Class<?> class1) {
		return this.input_format;
				
	}

	public Class<?> getOutputFromat(Class<?> class1) {
		return this.output_format;
	}

	public String getInput_file() {
		return input_file;
	}

	public void setInput_file(String input_file) {
		this.input_file = input_file;
	}

	public String getOutput_file() {
		return output_file;
	}

	public void setOutput_file(String output_file) {
		this.output_file = output_file;
	}
	
	
}
