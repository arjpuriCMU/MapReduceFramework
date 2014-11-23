JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	Config/ConfigSettings.java \
	Config/InternalConfig.java \
	DFS/ConnectionManagerInterface.java \
	DFS/DataNodeHeartbeatHelper.java \
	DFS/DataNodeInterface.java \
	DFS/DFSBlock.java \
	DFS/DFSConnectionManager.java \
	DFS/DFSDataNode.java \
	DFS/DFSFile.java \
	DFS/DFSHealthMonitor.java \
	DFS/DFSNameNode.java \
	DFS/DFSNameNodeInterface.java \
	DFS/HealthMonitor.java \
	DFS/HeartbeatHelperInterface.java \
	IOFormat/KeyValuePair.java \
	IOFormat/MRCollector.java \
	Main/Main.java \
	MapReduce/MapExecuter.java \
	MapReduce/Mapper.java \
	MapReduce/MapReducerClient.java \
	MapReduce/MapReducerConfig.java \
	MapReduce/ReduceExecuter.java \
	MapReduce/Reducer.java \
	MapReduce/TaskManager.java \
	MapReduce/TaskManagerInterface.java \
	Master/JobHandler.java \
	Master/MapReduceMasterInterface.java \
	Master/Master.java \
	Master/ScheduleManager.java \
	Master/ScheduleManagerInterface.java \
	Messages/Handshake.java \
	Messages/Heartbeat.java \
	Messages/Message.java \
	SumOfSquares/SumOfSquares.java \
	SumOfSquares/SumOfSquaresMap.java \
	SumOfSquares/SumOfSquaresReduce.java \
	Util/FileFunctions.java \
	Util/Host.java \
	Util/JavaCustomClassLoader.java \
	Util/Tuple.java \
	WordCount/WordCount.java \
	WordCount/WordCountMap.java \
	WordCount/WordCountReduce.java \


RMCLASSES = \
	Config/ConfigSettings.class \
	Config/InternalConfig.class \
	DFS/ConnectionManagerInterface.class \
	DFS/DataNodeHeartbeatHelper.class \
	DFS/DataNodeInterface.class \
	DFS/DFSBlock.class \
	DFS/DFSConnectionManager.class \
	DFS/DFSDataNode.class \
	DFS/DFSFile.class \
	DFS/DFSHealthMonitor.class \
	DFS/DFSNameNode.class \
	DFS/DFSNameNodeInterface.class \
	DFS/HealthMonitor.class \
	DFS/HeartbeatHelperInterface.class \
	IOFormat/KeyValuePair.class \
	IOFormat/MRCollector.class \
	Main/Main.class \
	MapReduce/MapExecuter.class \
	MapReduce/Mapper.class \
	MapReduce/MapReducerClient.class \
	MapReduce/MapReducerConfig.class \
	MapReduce/ReduceExecuter.class \
	MapReduce/Reducer.class \
	MapReduce/TaskManager.class \
	MapReduce/TaskManagerInterface.class \
	Master/JobHandler.class \
	Master/MapReduceMasterInterface.class \
	Master/Master.class \
	Master/ScheduleManager.class \
	Master/ScheduleManagerInterface.class \
	Messages/Handshake.class \
	Messages/Heartbeat.class \
	Messages/Message.class \
	SumOfSquares/SumOfSquares.class \
	SumOfSquares/SumOfSquaresMap.class \
	SumOfSquares/SumOfSquaresReduce.class \
	Util/FileFunctions.class \
	Util/Host.class \
	Util/JavaCustomClassLoader.class \
	Util/Tuple.class \
	WordCount/WordCount.class \
	WordCount/WordCountMap.class \
	WordCount/WordCountReduce.class \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
