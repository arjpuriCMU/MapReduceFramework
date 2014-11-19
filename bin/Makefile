JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	Config/ConfigSettings.java \
	Config/InternalConfig.java \
	DFS/ConnectionManagerInterface.java \
	DFS/DFSNameNodeInterface.java \
	DFS/HealthMonitor.java \
	DFS/HeartbeatHelperInterface.java \
	DFS/DataNodeInterface.java \
	DFS/DataNodeHeartbeatHelper.java \
	DFS/DFSBlock.java \
	DFS/DFSConnectionManager.java \
	DFS/DFSDataNode.java \
	DFS/DFSFile.java \
	DFS/DFSHealthMonitor.java \
	DFS/DFSNameNode.java \
	Main/Main.java \
	IOFormat/KeyValuePair.java \
	IOFormat/MapperCollector.java \
	IOFormat/ReducerCollector.java \
	Messages/Handshake.java \
	Messages/Heartbeat.java \
	Messages/Message.java \
	MapReduce/MapReduceInterface.java \
	MapReduce/MapReducer.java \
	MapReduce/Map.java \
	MapReduce/MapExecutor.java \
	MapReduce/Mapper.java \
	MapReduce/MapReducerConfig.java \
	MapReduce/ReducerExecuter.java \
	MapReduce/Reducer.java \
	MapReduce/TaskManager.java \
	MapReduce/TaskManagerInterface.java \
	Master/Master.java \
	Master/JobHandler.java \
	Master/MapReduceMasterInterface.java \
	Master/ScheduleManager.java \
	Master/ScheduleManagerInterface.java \
	Util/JavaCustomClassLoader.java \
	Util/FileFunctions.java \
	Util/Host.java \
	Util/Tuple.java \

RMCLASSES = \
	Config/ConfigSettings.class \
	Config/InternalConfig.class \
	DFS/ConnectionManagerInterface.class \
	DFS/DFSNameNodeInterface.class \
	DFS/HealthMonitor.class \
	DFS/HeartbeatHelperInterface.class \
	DFS/DataNodeInterface.class \
	DFS/DataNodeHeartbeatHelper.class \
	DFS/DFSBlock.class \
	DFS/DFSConnectionManager.class \
	DFS/DFSDataNode.class \
	DFS/DFSFile.class \
	DFS/DFSHealthMonitor.class \
	DFS/DFSNameNode.class \
	IOFormat/KeyValuePair.class \
	IOFormat/MapperCollector.class \
	IOFormat/ReducerCollector.class \
	MapReduce/Map.class \
	MapReduce/MapReduceInterface.class \
	MapReduce/MapReducer.class \
	MapReduce/MapExecutor.class \
	MapReduce/Mapper.class \
	MapReduce/MapReducerConfig.class \
	MapReduce/ReducerExecuter.class \
	MapReduce/Reducer.class \
	MapReduce/TaskManager.class \
	MapReduce/TaskManagerInterface.class \
	Main/Main.class \
	Master/Master.class \
	Master/MapReduceMasterInterface.class \
	Master/ScheduleManager.class \
	Master/ScheduleManagerInterface.class \
	Messages/Handshake.class \
	Messages/Heartbeat.class \
	Messages/Message.class \
	Util/FileFunctions.class \
	Util/Host.class \
	Util/Tuple.class \
	Util/JavaCustomClassLoader.class \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
