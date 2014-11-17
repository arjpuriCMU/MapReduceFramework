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
	Master/Master.java \
	Messages/Handshake.java \
	Messages/Heartbeat.java \
	Messages/Message.java \
	MapReduce/MapReduceInterface.java \
	MapReduce/MapReducer.java \
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
	Main/Main.class \
	Master/Master.class \
	Messages/Handshake.class \
	Messages/Heartbeat.class \
	Messages/Message.class \
	MapReduce/MapReduceInterface.class \
	MapReduce/MapReducer.class \
	Util/FileFunctions.class \
	Util/Host.class \
	Util/Tuple.class \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
