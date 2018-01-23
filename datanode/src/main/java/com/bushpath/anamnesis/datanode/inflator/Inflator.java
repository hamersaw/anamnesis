package com.bushpath.anamnesis.datanode.inflator;

import java.io.IOException;

public abstract class Inflator {
    public abstract byte[] inflate(double[] means, double[] standardDeviations,
        long recordCount) throws IOException;

    public abstract long getLength(double[] means, double[] standardDeviations,
        long recordCount);
}
