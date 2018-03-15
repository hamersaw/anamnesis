package com.bushpath.anamnesis.datanode.inflator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
                double value = means[j];
                if (!Double.isNaN(standardDeviations[j])) {
                    value += standardDeviations[j] * random.nextGaussian();
                }

                out.writeDouble(value);
            }
        }

        // close streams and write data
        out.close();
        byteOut.close();
        return byteOut.toByteArray();
    }

    @Override
    public void inflate(double[] means, double[] standardDeviations,
            long recordCount, ByteBuffer byteBuffer) throws IOException {
        Random random = new Random();

        // generating random records
        for (int i=0; i<recordCount; i++) {
            for (int j=0; j<means.length; j++) {
                double value = means[j];
                if (!Double.isNaN(standardDeviations[j])) {
                    value += standardDeviations[j] * random.nextGaussian();
                }

                byteBuffer.putDouble(value);
                //out.writeDouble(value);
            }
        }
    }

    @Override
    public long getLength(double[] means, double[] standardDeviations,
            long recordCount) {

        return recordCount * means.length * 8;
    }
}
