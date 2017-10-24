# anamnesis
## OVERVIEW
An in-memory, location aware, HDFS based file system.

## BUILD
#### CLIENT
> gradle build
> gradle depsJar

## EXECUTE
#### NAMENODE
> gradle execute -PappArgs="['src/main/resources/config.properties']"
#### DATANODE
> gradle execute -PappArgs="['src/main/resources/config1.properties']"
> gradle execute -PappArgs="['src/main/resources/config2.properties']"
> gradle execute -PappArgs="['src/main/resources/config3.properties']"
#### CLIENT
> ./anamnesis.sh mkdir foo/bar
> ./anamnesis.sh ls foo
> ./anamnesis.sh upload /tmp/test.txt foo/test.txt
> ./anamnesis.sh download foo/test.txt /tmp/test2.txt

## TODO
- implement datanode heartbeat storage information (integrate into addBlock)
- store data with datanode transfer
#### DATANODE TRANSFER
- saslStream (rfc 2222)?
- read/write ops to stream
    hadoop-hdfs
        org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.java
        org.apache.hadoop.hdfs.server.datanode.DataXceiver.java (extends Receiver)
        org.apache.hadoop.hdfs.server.datanode.BlockReceiver.java
        org.apache.hadoop.hdfs.server.datanode.BlockSender.java (comments on block transfer protocol)
    hadoop-hdfs-client
        datatransfer.proto
        org.apache.hadoop.hdfs.DFSPacket (packets for sending blocks over a link)
        org.apache.hadoop.hdfs.DFSInputStream (wrapper for receiving file from datanode)
        
        org.apache.hadoop.hdfs.DFSOutputStream (wrapper for sending file to datanode)
        org.apache.hadoop.hdfs.DataStreamer (actually sends data)
#### WRITE BLOCK PROTOCOL
    1. client -> (WriteOp) -> datanode
    2. client <- (Success) <- datanode
    3. client -> chunk -> datanode
        org.apache.hadoop.hdfs.DFSPacket
    4. client <- ack <- datanode
    5. repeat 3, 4 until finsihed
#### READ BLOCK PROTOCOL
    1. client -> (ReadOp) -> datanode
    2. client <- (Success) <- datanode
    3. client -> chunk -> datanode
        org.apache.hadoop.hdfs.server.datanode.BlockSender.java
    4. client <- ack <- datanode
    5. repeat 3, 4 until finished
