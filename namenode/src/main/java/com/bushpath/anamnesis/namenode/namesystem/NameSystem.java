package com.bushpath.anamnesis.namenode.namesystem;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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
        this.root = new NSDirectory("", Integer.MAX_VALUE, null);

        // TMP
        try {
            this.mkdir("/user", 666, false);
        } catch(Exception e) {}
    }

    public NSItem create(String path, int perm, String owner, 
            boolean createParent, long blockSize) throws Exception {
        // retrieve parent directory
        NSDirectory parentDirectory = getParentDirectory(path, createParent, perm);

        this.lock.writeLock().lock();
        try {
            // check if file already exists
            String[] elements = parseElements(path);
            if (parentDirectory.hasChild(elements[elements.length - 1])) {
                throw new StatusRuntimeException(Status.ALREADY_EXISTS
                    .withDescription("'" + path + "' already exists"));
            }
 
            // create new file and add as child of parent directory
            NSItem file = new NSFile(elements[elements.length - 1], owner,
                    owner, perm, blockSize, parentDirectory);
            parentDirectory.addChild(file);

            return file;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void complete(String path) throws Exception {
        NSItem file = getFile(path);
        if (file == null) {
            throw new Exception("file '" + path + "' does not exist");
        }

        this.lock.writeLock().lock();
        try {
            if (file.getType() != NSItem.Type.FILE) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("'" + path + "' is not a file"));
            }

            ((NSFile) file).complete();
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
        if (file == null) {
            throw new Exception("file '" + path + "' does not exist");
        }

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
                    throw new StatusRuntimeException(Status.UNIMPLEMENTED
                        .withDescription("file type '" + file.getType() 
                            + "' unimplemented"));
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
            // check if file already exists
            String[] elements = parseElements(path);
            if (parentDirectory.hasChild(elements[elements.length - 1])) {
                throw new StatusRuntimeException(Status.ALREADY_EXISTS
                    .withDescription("'" + path + "' already exists"));
            }
 
            // create new directory and add as child of parent directory
            NSItem dir = new NSDirectory(elements[elements.length - 1],
                perm, parentDirectory);
            parentDirectory.addChild(dir);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void rename(String srcPath, String dstPath) throws Exception {
        NSItem file = getFile(srcPath);
        if (file == null) {
            throw new Exception("file '" + srcPath + "' does not exist");
        }

        this.lock.writeLock().lock();
        try {
            NSDirectory oldParent = getParentDirectory(srcPath, false, 0);
            NSDirectory newParent = getParentDirectory(dstPath, false, 0);

            // remove from parents children
            oldParent.removeChild(file);

            // change file name
            String[] elements = parseElements(dstPath);
            file.setName(elements[elements.length - 1]);
     
            // add to new parents children
            newParent.addChild(file);
            file.setParent(newParent);
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
                if (current.getType() != NSItem.Type.DIRECTORY) {
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                        .withDescription("element '" + elements[i] 
                            + "' is not a directory"));
                }

                NSDirectory currentDirectory = (NSDirectory) current;
                if (!currentDirectory.hasChild(elements[i])) {
                    return null;
                } else {
                    current = currentDirectory.getChild(elements[i]);
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

            // parent directory of nothing is root
            if (elements.length == 0) {
                return current;
            }

            // iterate over path elements
            for (int i=0; i<elements.length - 1; i++) {
                if (current.hasChild(elements[i])) {
                    // check if item is a directory
                    NSItem item = current.getChild(elements[i]);
                    if (item.getType() != NSItem.Type.DIRECTORY) {
                        throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                            .withDescription("element '" + elements[i] 
                                + "' is not a directory"));
                    }

                    current = (NSDirectory) item;
                } else if (createParent) {
                    // create directory
                    NSDirectory newDirectory = new NSDirectory(elements[i],
                        perm, current);
                    current.addChild(newDirectory);
                    current = newDirectory;
                } else {
                    throw new StatusRuntimeException(Status.NOT_FOUND
                        .withDescription("'" + path + "' does not exist"));
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
