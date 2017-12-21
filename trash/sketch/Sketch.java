package com.bushpath.anamnesis.datanode.sketch;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class Sketch {
    protected double[] means;
    protected double[] standardDeviations;
    protected long recordCount;

    public Sketch(double[] means, double[] standardDeviations, long recordCount) {
        this.means = means;
        this.standardDeviations = standardDeviations;
        this.recordCount = recordCount;
    }

    public byte[] inflateToBytes() throws IOException {
        Random random = new Random();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);

        // generating random records
        for (int i=0; i<this.recordCount; i++) {
            for (int j=0; j<means.length; j++) {
                out.writeDouble(this.means[j]
                    + (this.standardDeviations[j] * random.nextGaussian()));
            }
        }

        // close streams and write data
        out.close();
        byteOut.close();
        return byteOut.toByteArray();
    }
}
