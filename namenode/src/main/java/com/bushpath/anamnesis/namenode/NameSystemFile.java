package com.bushpath.anamnesis.namenode;

import java.util.HashMap;
import java.util.Map;

public class NameSystemFile {
    public String name, owner, group;
    public boolean directory, file;
    public int perm;
    public long modificationTime, accessTime, length;

    public NameSystemFile parent;
    public Map<String, NameSystemFile> children;

    public NameSystemFile(String name, boolean directory, boolean file, 
            int perm, long length) {
        this.name = name;
        this.owner = "OWNER";
        this.group = "GROUP";
        this.directory = directory;
        this.file = file;
        this.perm = perm;
        this.modificationTime = System.currentTimeMillis();
        this.accessTime = modificationTime;
        this.length = length;

        this.children = new HashMap<>();
    }
}
