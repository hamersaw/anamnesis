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
> ./anamnesis.sh mkdir --path foo/bar
> ./anamnesis.sh ls --path foo
> ./anamnesis.sh upload --local-path /tmp/test.txt --path foo/test.txt
> ./anamnesis.sh download --path foo/test.txt --local-path /tmp/test2.txt

## TODO
- return errors from stream observer
- support favoredNodes on 'create' method (decide which node the data is written to)
- lots of refactoring (clean this stuff up)
- implement any kind of error handling!
- complete implementing creating new file
- implement datanode heartbeat storage information (integrate into addBlock)
- support needLocation flag on getListing
