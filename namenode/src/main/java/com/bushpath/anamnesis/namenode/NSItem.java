package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public abstract class NSItem {
    protected String name;
    protected Type type;
    protected int perm;

    public NSItem(String name, Type type, int perm) {
        this.name = name;
        this.type = type;
        this.perm = perm;
    }

    public String getName() {
        return this.name;
    }

    public NSItem.Type getType() {
        return this.type;
    }

    public int getPerm() {
        return this.perm;
    }

    public enum Type {
        DIRECTORY,
        FILE
    }

    public abstract HdfsProtos.HdfsFileStatusProto toHdfsFileStatusProto();
    public abstract void print(int indent);
}
