package com.bushpath.anamnesis.protocol;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

public class DatanodeProtocol {
    public static DatanodeProtocolProtos.RegisterDatanodeRequestProto
        buildRegisterDatanodeRequestProto(
            DatanodeProtocolProtos.DatanodeRegistrationProto registration) {

        return DatanodeProtocolProtos.RegisterDatanodeRequestProto.newBuilder()
            .setRegistration(registration)
            .build();
    }

    public static DatanodeProtocolProtos.RegisterDatanodeResponseProto
        buildRegisterDatanodeResponseProto(
            DatanodeProtocolProtos.DatanodeRegistrationProto registration) {

        return DatanodeProtocolProtos.RegisterDatanodeResponseProto.newBuilder()
            .setRegistration(registration)
            .build();
    }

    public static DatanodeProtocolProtos.DatanodeRegistrationProto
        buildDatanodeRegistrationProto(HdfsProtos.DatanodeIDProto datanodeID,
            HdfsServerProtos.StorageInfoProto storageInfo,
            HdfsServerProtos.ExportedBlockKeysProto keys, String softwareVersion) {

        return DatanodeProtocolProtos.DatanodeRegistrationProto.newBuilder()
            .setDatanodeID(datanodeID)
            .setStorageInfo(storageInfo)
            .setKeys(keys)
            .setSoftwareVersion(softwareVersion)
            .build();
    }
}
