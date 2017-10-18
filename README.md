# phoenix
## OVERVIEW
An in-memory, location aware, HDFS based file system.

## EXECUTE
#### NAMENODE
> gradle execute -PappArgs="['src/main/resources/config.properties']"
#### DATANODE
> gradle execute -PappArgs="['src/main/resources/config1.properties']"
#### CLIENT
> gradle execute -PappArgs="['mkdir','--path','foo/bar']"
> gradle execute -PappArgs="['mkdir','--path','foo/baz']"
> gradle execute -PappArgs="['ls','--path','foo']"

## TODO
- complete implementing creating new file
- combine code from create and mkdirs in NameSystem namenode code
- implement datanode heartbeat storage information (datanode side)
