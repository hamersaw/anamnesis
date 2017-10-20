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
- refactor datanodeManager update handling (cheeky as best)
- implement datanode heartbeat storage information (integrate into addBlock)
- write data transfer code for client / datanode
