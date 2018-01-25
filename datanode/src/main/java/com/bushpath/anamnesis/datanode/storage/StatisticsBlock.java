package com.bushpath.anamnesis.datanode.storage;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class StatisticsBlock extends Block {
    protected double[][] means;
    protected double[][] standardDeviations;
    protected long[] recordCounts;
    protected Inflator inflator;
    
    public StatisticsBlock(long blockId, long generationStamp, double[][] means, 
            double[][] standardDeviations, long[] recordCounts,
            Inflator inflator, boolean justInTimeInflation) throws IOException {
        super(blockId, generationStamp, null);

        this.means = means;
        this.standardDeviations = standardDeviations;
        this.recordCounts = recordCounts;
        this.inflator = inflator;

        // if not justInTimeInflation -> inflate()
        if (!justInTimeInflation) {
            this.inflate();
        }
    }

    @Override
    public byte[] getBytes() throws IOException {
        // if not memory resident -> compute
        if (this.bytes == null) {
            this.inflate();
        }

        return this.bytes;
    }

    @Override
    public long getLength() {
        // if memory resident -> return length
        if (this.bytes != null) {
            return this.bytes.length;
        }

        // otherwise compute length
        long length = 0;
        for (int i=0; i<this.recordCounts.length; i++) {
            length += this.inflator.getLength(this.means[i],
                this.standardDeviations[i], this.recordCounts[i]);
        }

        return length;
    }

    public void inflate() throws IOException {
        if (bytes != null) {
            return;
        }

        // compute bytes
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

        for (int i=0; i<this.recordCounts.length; i++) {
            byte[] bytes = this.inflator.inflate(this.means[i],
                this.standardDeviations[i], this.recordCounts[i]);

            bytesOut.write(bytes);
        }

        this.bytes = bytesOut.toByteArray();
        bytesOut.close();
    }
}
