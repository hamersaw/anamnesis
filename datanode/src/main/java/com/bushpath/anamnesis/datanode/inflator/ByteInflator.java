package com.bushpath.anamnesis.datanode.inflator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class ByteInflator extends Inflator {
    @Override
    public byte[] inflate(double[] means, double[] standardDeviations,
            long recordCount) throws IOException {
        Random random = new Random();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);

        // generating random records
        for (int i=0; i<recordCount; i++) {
            for (int j=0; j<means.length; j++) {
                out.writeDouble(means[j]
                    + (standardDeviations[j] * random.nextGaussian()));
            }
        }

        // close streams and write data
        out.close();
        byteOut.close();
        return byteOut.toByteArray();
    }
}
