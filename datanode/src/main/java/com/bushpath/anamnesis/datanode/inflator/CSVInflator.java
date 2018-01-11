package com.bushpath.anamnesis.datanode.inflator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class CSVInflator extends Inflator {
    @Override
    public byte[] inflate(double[] means, double[] standardDeviations,
            long recordCount) throws IOException {
        Random random = new Random();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);

        // generating random records
        for (int i=0; i<recordCount; i++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j=0; j<means.length; j++) {
                double value = means[j];
                if (!Double.isNaN(standardDeviations[j])) {
                    value += standardDeviations[j] * random.nextGaussian();
                }

                stringBuilder.append((j != 0 ? "," : "") + value);
            }
            stringBuilder.append("\n");

            out.write(stringBuilder.toString().getBytes());
        }

        // close streams and write data
        out.close();
        byteOut.close();
        return byteOut.toByteArray();
    }
}
