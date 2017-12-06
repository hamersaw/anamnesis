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
- change DatanodeService to add Datanode not it's elements
- setup some rock solid logging
- implement datanode heartbeat storage information (integrate into addBlock)
#### RPC SERVER
- rpc response error handling (fix up)
#### CLIENT
- fix everything (works with hdfs native client)

- have 3 rotating buffers for reading chunks
    byte[][] (currentBuffer = 0 /1 /2)


# WRITE TIME TEST
## BASE
9.11user 1.19system 0:56.43elapsed 18%CPU (0avgtext+0avgdata 257632maxresident)k
1240inputs+104outputs (14major+68455minor)pagefaults 0swaps
## INCREASING BLOCK BUFFER (DataTransferService)
9.16user 2.20system 0:59.71elapsed 19%CPU (0avgtext+0avgdata 257732maxresident)k
5200280inputs+104outputs (0major+67352minor)pagefaults 0swaps
## REMOVE CHECKSUM VALIDATION
8.74user 1.78system 0:26.95elapsed 39%CPU (0avgtext+0avgdata 261692maxresident)k
934152inputs+64outputs (2major+66814minor)pagefaults 0swap
## READ CHECKSUMS INTO ARRAY (INSTEAD OF readInt)
8.70user 1.89system 0:14.19elapsed 74%CPU (0avgtext+0avgdata 250064maxresident)k
318632inputs+64outputs (0major+63219minor)pagefaults 0swaps
