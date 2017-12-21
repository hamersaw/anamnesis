package com.bushpath.anamnesis.datanode.ipc.rpc;

import com.google.protobuf.Message;

import com.bushpath.anamnesis.datanode.inflator.Inflator;
import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamensis.protocol.proto.DatanodeSketchProtocolProtos;

import java.io.DataInputStream;
import java.util.List;

public class DatanodeSketchService {
    private Inflator inflator;
    private Storage storage;

    public DatanodeSketchService(Inflator inflator, Storage storage) {
        this.inflator = inflator;
        this.storage = storage;
    }

    public Message sendStats(DataInputStream in) throws Exception {
        DatanodeSketchProtocolProtos.SendStatsRequestProto req =
            DatanodeSketchProtocolProtos.SendStatsRequestProto
                .parseDelimitedFrom(in);

        // inflate stats
        List<Double> meansList = req.getMeansList();
        double[] means = new double[meansList.size()];
        for (int i=0; i<meansList.size(); i++) {
            means[i] = meansList.get(i).doubleValue();
        }

        List<Double> standardDeviationsList = req.getStandardDeviationsList();
        double[] standardDeviations = new double[standardDeviationsList.size()];
        for (int i=0; i<standardDeviationsList.size(); i++) {
            standardDeviations[i] = standardDeviationsList.get(i).doubleValue();
        }

        byte[] block = this.inflator.inflate(means,
            standardDeviations, req.getRecordCount());

        storage.storeBlock(req.getBlockId(), block, req.getGenerationStamp());

        // respond to request
        return DatanodeSketchProtocolProtos.SendStatsResponseProto
            .newBuilder()
            .build();
    }
}
