JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	Config/ConfigSettings.java \
	DFS/DataNodeHeartbeatHelper.java \
	DFS/DFSBlock.java \
	DFS/DFSConfig.java \
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
	Util/FileFunctions.java \
	Util/Host.java \
	Util/Tuple.java \

RMCLASSES = \
	Config/ConfigSettings.class \
	DFS/DataNodeHeartbeatHelper.class \
	DFS/DFSBlock.class \
	DFS/DFSConfig.class \
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
	Util/FileFunctions.class \
	Util/Host.class \
	Util/Tuple.class \


default: classes

classes: $(CLASSES:.java=.class)

clean:	
	rm $(RMCLASSES)
