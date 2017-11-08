# anamnesis
## OVERVIEW
An in-memory, location aware, HDFS based file system.

## BUILD
#### EACH COMPONENT (CLIENT, NAMENODE, DATANODE)
> gradle build
> gradle depsJar

## EXECUTE
#### NAMENODE
> ./namenode.sh src/main/resources/config.properties
#### DATANODE
> ./datanode.sh src/main/resources/config1.properties
> ./datanode.sh src/main/resources/config2.properties
> ./datanode.sh src/main/resources/config3.properties
#### CLIENT
> ./anamnesis.sh mkdir foo/bar
> ./anamnesis.sh ls foo
> ./anamnesis.sh upload /tmp/test.txt foo/test.txt -b 2 -f 0
> ./anamnesis.sh download foo/test.txt /tmp/test2.txt

## SPARK
> ./sbin/start-master.sh
> ./sbin/start-slave.sh localhost:7077

## TODO
- get rid of stupid byte[] buffer in ChunkPacket
    keep separate for data and checksums (no longer need to put header in it)
- figure out why we're adding 4 to packet length in ChunkPacket (DFSPacket)
- implement datanode heartbeat storage information (integrate into addBlock)
- write checksums DataOutputStream.write()
- validate checksums in ChunkPacket.read()
#### RPC SERVER
- rpc response error handling (fix up)
- implement all functionality
- write client for datanode -> namenode communication
