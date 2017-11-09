package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class NSDirectory extends NSItem {
    private Map<String, NSItem> children;

    public NSDirectory(String name, int perm, NSItem parent) {
        super(name, NSItem.Type.DIRECTORY, perm, parent);
        this.children = new TreeMap<>();
    }

    public boolean hasChild(String name) {
        return this.children.containsKey(name);
    }

    public NSItem getChild(String name) {
        return this.children.get(name);
    }

    public void addChild(NSItem child) {
        this.children.put(child.getName(), child);
    }

    public void removeChild(NSItem child) {
        this.children.remove(child.getName());
    }

    public Collection<NSItem> getChildren() {
        List<NSItem> list = new ArrayList<>();
        for (Map.Entry<String, NSItem> entry : this.children.entrySet()) {
            list.add(entry.getValue());
        }

        return list;
    }

    @Override
    public HdfsProtos.HdfsFileStatusProto toHdfsFileStatusProto(boolean needLocation) {
        HdfsProtos.FsPermissionProto permission = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(this.perm)
                .build();

        return HdfsProtos.HdfsFileStatusProto.newBuilder()
            .setFileType(HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR)
            .setPath(ByteString.copyFrom(this.getPath().getBytes()))
            .setLength(-1)
            .setPermission(permission)
            .setOwner("")
            .setGroup("")
            .setModificationTime(this.modificationTime)
            .setAccessTime(this.accessTime)
            .setChildrenNum(this.children.size())
            .build();
    }

    @Override
    public void print(int indent) {
        for (int i=0; i<indent; i++) {
            System.out.print("\t");
        }

        System.out.println("DIR:" + this.name);
        for (NSItem child: this.children.values()) {
            child.print(indent + 1);
        }
    }
}
