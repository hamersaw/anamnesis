# phoenix
## OVERVIEW
An in-memory, location aware, HDFS based file system.

## EXECUTE
#### NAMENODE
> gradle run
#### DATANODE
> TODO
#### CLIENT
> gradle execute -PappArgs="['mkdir','--path','foo/bar']"
> gradle execute -PappArgs="['mkdir','--path','foo/baz']"
> gradle execute -PappArgs="['ls','--path','foo']"

## TODO
- configuration files for datanode and namenode (java resource?)
- combine code from create and mkdirs in NameSystem namenode code
- set file path name in Hdfs.HdfsFileStatusProto (ls command)
- implement datanode heartbeat storage information (datanode side)
