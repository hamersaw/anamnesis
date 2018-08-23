package com.bushpath.anamnesis.datanode.inflator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class CSVInflator extends Inflator {
    public static final int LENGTH = 10;
    private Random random;

    public CSVInflator() {
        this.random = new Random(System.currentTimeMillis());
    }

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

                // TODO - optimize this
                String valueString = Double.toString(value);
                int stringLength = valueString.length();
                if (stringLength > LENGTH) {    
                    valueString = valueString.substring(0, LENGTH);
                } else if (stringLength < LENGTH) {
                    valueString = String.format("%1$-" + LENGTH + "s", valueString)
                        .replace(' ', '0'); 
                }

                stringBuilder.append((j != 0 ? "\t" : "") + valueString);
            }
            stringBuilder.append("\n");

            out.write(stringBuilder.toString().getBytes());
        }

        // close streams and write data
        out.close();
        byteOut.close();
        return byteOut.toByteArray();
    }

    @Override
    public void inflate(double[][] means, double[][] standardDeviations,
            long[] recordCount, ByteBuffer byteBuffer) throws IOException {

        // generating random records
        for (int x=0; x<means.length; x++) {
            for (int i=0; i<recordCount[x]; i++) {
                for (int j=0; j<means[x].length; j++) {
                    double value = means[x][j];
                    if (!Double.isNaN(standardDeviations[x][j])) {
                        value += standardDeviations[x][j] * random.nextGaussian();
                    }

                    if (j != 0) {
                        byteBuffer.put((byte) ',');
                    }

                    String valueString = Double.toString(value);
                    for (int k=0; k<LENGTH; k++) {
                        if (k < valueString.length()) {
                            byteBuffer.put((byte) valueString.charAt(k));
                        } else {
                            byteBuffer.put((byte) '0');
                        }
                    }
                }

                byteBuffer.put((byte) '\n');
            }
        }
    }

    @Override
    public long getLength(double[] means, double[] standardDeviations,
            long recordCount) {

        // recordCount *                total number of records
        // ((means.length * LENGTH)     number of characters for each value
        // + means.length)              tab an newline for each value
        return recordCount * ((means.length * LENGTH) + means.length);
    }
}
