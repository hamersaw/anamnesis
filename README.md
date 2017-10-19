# anamnesis
## OVERVIEW
An in-memory, location aware, HDFS based file system.

## EXECUTE
#### NAMENODE
> gradle execute -PappArgs="['src/main/resources/config.properties']"
#### DATANODE
> gradle execute -PappArgs="['src/main/resources/config1.properties']"
> gradle execute -PappArgs="['src/main/resources/config2.properties']"
> gradle execute -PappArgs="['src/main/resources/config3.properties']"
#### CLIENT
> gradle execute -PappArgs="['mkdir','--path','foo/bar']"
> gradle execute -PappArgs="['ls','--path','foo']"
> gradle execute -PappArgs="['upload','--local-path','/tmp/test.txt','--path','test.txt']"
> gradle execute -PappArgs="['download','--local-path','/tmp/test2.txt','--path','test.txt']"

## TODO
- lots of refactoring (clean this stuff up)
- implement any kind of error handling!
- complete implementing creating new file
- implement datanode heartbeat storage information (integrate into addBlock)
- support needLocation flag on getListing
