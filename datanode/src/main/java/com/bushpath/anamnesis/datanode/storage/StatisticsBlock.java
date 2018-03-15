package com.bushpath.anamnesis.datanode.storage;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class StatisticsBlock extends Block {
    private static final Logger logger =
        Logger.getLogger(StatisticsBlock.class.getName());

    protected double[][] means;
    protected double[][] standardDeviations;
    protected long[] recordCounts;
    protected Inflator inflator;
    
    public StatisticsBlock(long blockId, long generationStamp, double[][] means, 
            double[][] standardDeviations, long[] recordCounts,
            Inflator inflator) throws IOException {
        super(blockId, generationStamp, null);

        this.means = means;
        this.standardDeviations = standardDeviations;
        this.recordCounts = recordCounts;
        this.inflator = inflator;
    }

    @Override
    public byte[] getBytes() throws IOException {
        this.addAccess();

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

    public boolean isInflated() {
        return this.bytes != null;
    }

    public void inflate() throws IOException {
        if (this.bytes != null) {
            return;
        }

        /*// compute bytes
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

        long startTime = System.currentTimeMillis();
        long totalRecordCount = 0;
        for (int i=0; i<this.recordCounts.length; i++) {
            totalRecordCount += this.recordCounts[i];
            byte[] bytes = this.inflator.inflate(this.means[i],
                this.standardDeviations[i], this.recordCounts[i]);

            bytesOut.write(bytes);
        }

        logger.info("block " + this.blockId + ": generated " + totalRecordCount
            + " record(s) in " + (System.currentTimeMillis() - startTime) + " ms");

        this.bytes = bytesOut.toByteArray();
        bytesOut.close();*/

        ByteBuffer byteBuffer = ByteBuffer.allocate((int) this.getLength());
        long startTime = System.currentTimeMillis();
        long totalRecordCount = 0;
        for (int i=0; i<this.recordCounts.length; i++) {
            totalRecordCount += this.recordCounts[i];
            this.inflator.inflate(this.means[i], this.standardDeviations[i],
                this.recordCounts[i], byteBuffer);
        }

        logger.info("block " + this.blockId + ": generated " + totalRecordCount
            + " record(s) in " + (System.currentTimeMillis() - startTime) + " ms");

        this.bytes = byteBuffer.array();
    }

    public void evict() {
        this.bytes = null;
    }
}
