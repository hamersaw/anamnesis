package com.bushpath.anamnesis.datanode;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.datanode.storage.Block;
import com.bushpath.anamnesis.datanode.storage.StatisticsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimerTask;

public class MemoryEvictionTask extends TimerTask {
    protected Configuration configuration;
    protected Storage storage;

    public MemoryEvictionTask(Configuration configuration, Storage storage) {
        this.configuration = configuration;
        this.storage = storage;
    }

    @Override
    public void run() {
        // check if memory usage is over threshold
        Runtime runtime = Runtime.getRuntime();
        double maxMemory = runtime.maxMemory();
        double usedMemory = maxMemory - runtime.freeMemory();
        if (usedMemory / maxMemory < this.configuration.maxMemoryThreshold) {
            return;
        }
 
        // retrieve StatisticBlocks
        List<StatisticsBlock> blocks = new ArrayList<>();
        for (Block block : this.storage.getBlocks()) {
            if(block instanceof StatisticsBlock) {
                blocks.add((StatisticsBlock) block);
            }
        }

        // sort eviction list
        long beginAccessTime = System.currentTimeMillis()
            - (this.configuration.blockAccessTimeDelta * 1000);
        Collections.sort(blocks, new Comparator<StatisticsBlock>() {
            @Override
            public int compare(StatisticsBlock b1, StatisticsBlock b2) {
                int a1 = getAccessTimesSince(beginAccessTime, b1.getAccessTimes());
                int a2 = getAccessTimesSince(beginAccessTime, b2.getAccessTimes());

                // order by number of accesses increasing
                if (a1 > a2) {
                    return 1;
                } else if (a1 < a2) {
                    return -1;
                } else {
                    // order them by size decreasing
                    return (int) b2.getLength() - (int) b1.getLength();
                }
            }
        });

        // evict blocks
        for (StatisticsBlock block : blocks) {
            block.evict();
            usedMemory -= block.getLength();

            // if we are now below the minimum memory threshold -> break
            if (usedMemory / maxMemory < this.configuration.minMemoryThreshold) {
                break;
            }
        }
    }

    private int getAccessTimesSince(long value, List<Long> accessTimes) {
        int count = 0;
        for (long accessTime : accessTimes) {
            if (accessTime >= value) {
                count += 1;
            }
        }

        return count;
    }
}
