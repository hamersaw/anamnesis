package com.bushpath.anamnesis.namenode;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class NameSystem {
    private static final Logger logger = Logger.getLogger(NameSystem.class.getName());

    protected ReadWriteLock lock;
    protected NSDirectory root;

    public NameSystem() {
        this.lock = new ReentrantReadWriteLock();
        this.root = new NSDirectory("", Integer.MAX_VALUE);
    }

    public void create(String path, int perm, String owner, 
            boolean createParent, long blockSize) throws Exception {
        // retrieve parent directory
        NSDirectory parentDirectory = getParentDirectory(path, createParent, perm);

        this.lock.writeLock().lock();
        try {
            // create new file and add as child of parent directory
            String[] elements = parseElements(path);
            NSItem file = new NSFile(elements[elements.length - 1], owner,
                    "", perm, blockSize);
            parentDirectory.addChild(file);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Collection<NSItem> getListing(String path) throws Exception {
        // check if path is root
        if (path.replaceAll("/", "").isEmpty()) {
            return this.root.getChildren();
        }

        NSItem file = getFile(path);

        this.lock.readLock().lock();
        try {
            switch (file.getType()) {
                case DIRECTORY:
                    return ((NSDirectory) file).getChildren();
                case FILE:
                    Set<NSItem> files = new HashSet<>();
                    files.add(file);
                    return files;
                default:
                    throw new Exception("unknown file type for file '" + path + "'");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void mkdir(String path, int perm, boolean createParent) throws Exception {
        // retrieve parent directory
        NSDirectory parentDirectory = getParentDirectory(path, createParent, perm);

        this.lock.writeLock().lock();
        try {
            // create new directory and add as child of parent directory
            String[] elements = parseElements(path);
            NSItem dir = new NSDirectory(elements[elements.length - 1], perm);
            parentDirectory.addChild(dir);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public NSItem getFile(String path) throws Exception {
        this.lock.readLock().lock();
        try {
            NSItem current = this.root;
            String[] elements = parseElements(path);

            // iterate over path elements
            for (int i=0; i<elements.length; i++) {
                if (current.getType() != NSItem.Type.DIRECTORY ||
                        !((NSDirectory) current).hasChild(elements[i])) {
                    throw new Exception("file '" + path + "' does not exists");
                } else {
                    current = ((NSDirectory) current).getChild(path);
                }
            }

            return current;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    private NSDirectory getParentDirectory(String path, 
            boolean createParent, int perm) throws Exception {
        this.lock.writeLock().lock();
        try {
            NSDirectory current = this.root;
            String[] elements = parseElements(path);

            // iterate over path elements
            for (int i=0; i<elements.length - 1; i++) {
                if (current.hasChild(elements[i])) {
                    // check if item is a directory
                    NSItem item = current.getChild(elements[i]);
                    if (item.getType() != NSItem.Type.DIRECTORY) {
                        throw new Exception("path element '" + elements[i]
                                + "' is not a directory");
                    }

                    current = (NSDirectory) item;
                } else if (createParent) {
                    // create directory
                    NSDirectory newDirectory = new NSDirectory(elements[i], perm);
                    current.addChild(newDirectory);
                    current = newDirectory;
                } else {
                    throw new Exception("path element '" + elements[i]
                            + "' does not exist");
                }
            }

            return current;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private String[] parseElements(String path) {
        return path.replaceAll("/$", "").replaceAll("^/", "").split("/");
    }
}
