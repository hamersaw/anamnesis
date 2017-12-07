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
> ./datanode.sh src/main/resources/config4.properties
#### CLIENT
> ./anamnesis.sh mkdir foo/bar
> ./anamnesis.sh ls foo
> ./anamnesis.sh upload /tmp/test.txt foo/test.txt -b 2 -f 0
> ./anamnesis.sh download foo/test.txt /tmp/test2.txt

## HDFS
> ./bin/hdfs dfs -mkdir /user/hamersaw
> ./bin/hdfs dfs -D dfs.block.size=1024 -copyFromLocal /tmp/test.txt /user/hamersaw/test.txt
> ./bin/hdfs dfs -copyToLocal /user/hamersaw/test.txt /tmp/test2.txt

## SPARK
$bin/spark-shell
scala> val rdd = sc.textFile("hdfs://localhost/user/hamersaw/MOCK_DATA.csv")

## TODO
- integrate nativecrc32 bulkCompute into readBlock
- change DatanodeService to add Datanode not it's elements
- setup some rock solid logging
- implement datanode heartbeat storage information (integrate into addBlock)
#### RPC SERVER
- rpc response error handling (fix up)
#### CLIENT
- fix everything (works with hdfs native client)
