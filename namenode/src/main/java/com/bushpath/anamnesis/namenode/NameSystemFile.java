package com.bushpath.anamnesis.namenode;

import java.util.HashMap;
import java.util.Map;

public class NameSystemFile {
    public String name, owner, group;
    public boolean directory, file;
    public int perm;
    public long modificationTime, accessTime, length, blockSize;

    public NameSystemFile parent;
    public Map<String, NameSystemFile> children;

    public static NameSystemFile newDirectory(String name, String owner, String group, 
            int perm) {

        NameSystemFile file = new NameSystemFile();
        file.name = name;
        file.owner = owner;
        file.group = group;
        file.directory = true;
        file.file = false;
        file.perm = perm;
        file.modificationTime = System.currentTimeMillis();
        file.accessTime = file.modificationTime;
        file.length = -1;
        file.blockSize = -1;

        file.children = new HashMap<>();
        return file;
    }

    public static NameSystemFile newFile(String name, String owner, String group,
            int perm, long blockSize) {

        NameSystemFile file = new NameSystemFile();
        file.name = name;
        file.owner = owner;
        file.group = group;
        file.directory = false;
        file.file = true;
        file.perm = perm;
        file.modificationTime = System.currentTimeMillis();
        file.accessTime = file.modificationTime;
        file.length = 0;
        file.blockSize = blockSize;
        return file;
    }
}
