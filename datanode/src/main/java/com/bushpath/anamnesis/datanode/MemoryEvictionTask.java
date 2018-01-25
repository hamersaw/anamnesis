package com.bushpath.anamnesis.datanode;

import com.bushpath.anamnesis.datanode.storage.Storage;

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
        // TODO - periodically monitor memeory
    }
}
