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
- implement any kind of error handling!
- complete implementing creating new file
- implement datanode heartbeat storage information (integrate into addBlock)
