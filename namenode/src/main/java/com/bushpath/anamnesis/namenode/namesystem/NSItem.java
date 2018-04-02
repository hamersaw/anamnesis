package com.bushpath.anamnesis.namenode.namesystem;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public abstract class NSItem {
    protected String name;
    protected Type type;
    protected int perm;
    protected String owner, group;
    protected long modificationTime, accessTime;
    protected NSItem parent;

    public NSItem(String name, Type type, int perm, String owner,
            String group, NSItem parent) {
        this.name = name;
        this.type = type;
        this.perm = perm;
        this.owner = owner;
        this.group = group;
        this.parent = parent;

        this.modificationTime = System.currentTimeMillis();
        this.accessTime = this.modificationTime;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        if (this.parent == null) {
            return "";
        } else {
            return this.parent.getPath() + "/" + this.name;
        }
    }

    public NSItem.Type getType() {
        return this.type;
    }

    public int getPerm() {
        return this.perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public String getOwner() {
        return this.owner;
    }

    public String getGroup() {
        return this.group;
    }

    public long getModificationTime() {
        return this.modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    public long getAccessTime() {
        return this.accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    public void setParent(NSItem parent) {
        this.parent = parent;
    }

    public enum Type {
        DIRECTORY,
        FILE
    }

    public abstract HdfsProtos.HdfsFileStatusProto
        toHdfsFileStatusProto(boolean needLocation);
    public abstract void print(int indent);
}
