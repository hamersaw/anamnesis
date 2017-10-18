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
    protected NameSystemFile root;

    public NameSystem() {
        this.lock = new ReentrantReadWriteLock();
        this.root = new NameSystemFile("", true, false, Integer.MAX_VALUE, 0);
    }

    public void create(String path, int perm) throws Exception {
        this.lock.writeLock().lock();
        try {
            logger.info("TODO");
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Collection<NameSystemFile> getListing(String path) throws Exception {
        this.lock.readLock().lock();
        try {
            NameSystemFile current = this.root;
            String[] elements = parseElements(path);

            // find file refered to by path
            for (int i=0; i<elements.length; i++) {
                if (!current.children.containsKey(elements[i])) {
                    throw new Exception("directory doesn't exist");
                }

                current = current.children.get(elements[i]);
            }

            if (current.file) {
                // if a file -> return file
                Set<NameSystemFile> files = new HashSet<>();
                files.add(current);
                return files;
            } else {
                // if a directory -> return children
                return current.children.values();
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void mkdir(String path, int perm, boolean createParent) throws Exception {
        this.lock.writeLock().lock();
        try {
            NameSystemFile current = this.root;
            String[] elements = parseElements(path);

            // iterate over path elements
            for (int i=0; i<elements.length; i++) {
                if (i == elements.length - 1) {
                    // last element
                    if (current.children.containsKey(elements[i])) {
                        throw new Exception("directory already exists");
                    } else {
                        // create directory
                        NameSystemFile f = 
                            new NameSystemFile(elements[i], true, false, perm, 0);
                        f.parent = current;
                        current = current.children.put(elements[i], f);
                    }
                } else {
                    // not last element
                    if (current.children.containsKey(elements[i])) {
                        current = current.children.get(elements[i]);
                    } else if (createParent) {
                        // create missing directory
                        NameSystemFile f = 
                            new NameSystemFile(elements[i], true, false, perm, 0);
                        f.parent = current;
                        current.children.put(elements[i], f);
                        current = f;
                    } else {
                        throw new Exception("parent directory doesn't exist");
                    }
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private String[] parseElements(String path) {
        return path.replaceAll("/$", "").replaceAll("^/", "").split("/");
    }
}
