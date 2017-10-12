# APPROACHES
- both require separate API for clients to tell namenodes where to store data
    (is this true? can we just use clients to store data wherever we want?)
## IMPLEMENT PROTOBUF PROTOCOLS
- extremely lightweight
- various backend storage systems
- 1-2 weeks
## EXTEND HDFS IMPLEMENTATION
- could be quite difficult (tons of unknown / undocumented code)
- gives lots of uneccessary functionality
- ~ 3 weeks

# BUILD ADDITIONAL PROTOCOLS ON TOP?
- **choose which node to write data to**
- view datanode resource (consumption)

# PROTOCOLS
DatanodeLifelineProtocol (1)
DatanodeProtocol (9)
InterDatanodeProtocol (2)

NamenodeProtocol (12)
JournalProtocol (3)
QJournalProtocol (19)

ClientDatanodeProtocol (14)
ClientNamenodeProtocol (86)

# ARCHITECTURE
client
    Client.java                         base class
    protocol
        ClientDataNodeClient.java       client for datanode communication
        ClientNameNodeClient.java       client for namenode communication
datanode
    BlockManager.java                   handles serving/storing block locations on the FS
    DataNode.java                       base class
    protocol
        ClientDataNodeService.java      service to handle client requests
        DataNodeClient.java             client for namenode communication (datanode protocol)
    storage
        FileSystem.java                 abstract file system class
        JavaMemoryFS.java               store files in java memory
        SharedMemoryFS.java             shared memory filesystem (JNI)
        TmpfsFS.java                    tmpfs file system
namenode
    FSNamesystem.java                   handles namesystem for files
    NameNode.java                       base class
    protocol
        ClientNameNodeService.java      service to handle client requests
        DataNodeService.java            service to handle datanode requests
        NameNodeClient.java             client for namenode communication
        NameNodeService.java            service to handle namenode requests
