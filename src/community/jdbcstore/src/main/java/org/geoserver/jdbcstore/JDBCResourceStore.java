/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.jdbcstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.io.output.ProxyOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.geoserver.jdbcstore.cache.ResourceCache;
import org.geoserver.jdbcstore.internal.JDBCDirectoryStructure;
import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.geoserver.platform.resource.LockProvider;
import org.geoserver.platform.resource.NullLockProvider;
import org.geoserver.platform.resource.Resource;
import org.geoserver.platform.resource.ResourceListener;
import org.geoserver.platform.resource.ResourceStore;
import org.geoserver.platform.resource.Resources;

/**
 * Implementation of ResourceStore backed by a JDBC DirectoryStructure.
 * 
 * @author Kevin Smith, Boundless
 * @author Niels Charlier
 *
 */
public class JDBCResourceStore implements ResourceStore {
    
    private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger(JDBCResourceStore.class);
    
    /** LockProvider used to secure resources for exclusive access */
    //TODO: clustering supported lock mechanism
    protected LockProvider lockProvider = new NullLockProvider();
            
    protected JDBCDirectoryStructure dir;    
    protected ResourceCache cache;
    protected ResourceStore oldResourceStore;
    
    public void setCache(ResourceCache cache) {
        this.cache = cache;
    }
       
   /**
    * Configure LockProvider used during {@link Resource#out()}.
    * 
    * @param lockProvider LockProvider used for Resource#out()    
    */
   public void setLockProvider(LockProvider lockProvider) {
       this.lockProvider = lockProvider;
   }
    
    public JDBCResourceStore(JDBCDirectoryStructure dir) {
        this.dir = dir;
    }
    
    public JDBCResourceStore(DataSource ds, JDBCResourceStoreProperties config) {
        this(new JDBCDirectoryStructure(ds, config));
    }
    
    public JDBCResourceStore(DataSource ds, JDBCResourceStoreProperties config, ResourceStore oldResourceStore) {
        this(ds, config);
        this.oldResourceStore = oldResourceStore;
        
        if (config.isImport()) {
            if (oldResourceStore != null) {
                try {
                    Resource root = oldResourceStore.get("");
                    for (Resource child : root.list()) {
                        if  (!ArrayUtils.contains(config.getIgnoreDirs(), child.name())) {
                            Resources.copy(child, get(child.name()));
                        }
                    }
                    config.setImport(false);
                    config.save();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                LOGGER.warning("Cannot import resources: no old resource store available.");
            }   
        }
    }
    
    @Override
    public Resource get(String path) {
        if (oldResourceStore != null && ArrayUtils.contains(dir.getConfig().getIgnoreDirs(), path)) {
            return oldResourceStore.get(path);
        }
        return new JDBCResource(dir.createEntry(path));
    }
        
    @Override
    public boolean remove(String path) {
        return dir.createEntry(path).delete();
    }
    
    @Override
    public boolean move(String path, String target) {
        return dir.createEntry(path).renameTo(dir.createEntry(target));
    }
    
    @Override
    public String toString() {
        return "JDBCResourceStore";
    }

    /**
     * Direct implementation of Resource.
     * 
     */    
    protected class JDBCResource implements Resource {
        
        private final JDBCDirectoryStructure.Entry entry;
        
        public JDBCResource(JDBCDirectoryStructure.Entry entry) {
            assert(entry != null);
            this.entry = entry;
        }
        
        @Override
        public String path() {
            return entry.toString();
        }

        @Override
        public String name() {
            return entry.getName();
        }
        
        private InputStream getIStream() {
            return entry.getContent();            
        }
                
        @Override
        public InputStream in() {
            final Lock lock = lock();
            try {
                entry.createResource();
                return getIStream();
            } finally {
                lock.release();
            }
        }

        @Override
        public OutputStream out() {
            entry.createResource();
            try {
                return new CachingOutputStreamWrapper(File.createTempFile("out.", entry.getName()));
            } catch (IOException ex) {
                throw new IllegalStateException(ex); 
            }
        }

        @Override
        public File file() {
            if (getType() == Type.DIRECTORY) {
                throw new IllegalStateException("Directory (not a file)");
            }
            final Lock lock = lock();
            try {
                return cache.cache(this, false);
            } catch(IOException ex) {
                throw new IllegalStateException(ex);
            } finally {
                lock.release();
            }
        }

        @Override
        public File dir() {
            if (getType() == Type.RESOURCE) {
                throw new IllegalStateException("File (not a directory)");
            }
            final Lock lock = lock();
            try {
                return cache.cache(this, true);
            } catch(IOException ex) {
                throw new IllegalStateException(ex);
            } finally {
                lock.release();
            }
        }

        @Override
        public long lastmodified() {
            Timestamp ts = entry.getLastModified();
            return ts == null ? 0L : ts.getTime();
        }

        @Override
        public Resource parent() {
            return new JDBCResource(entry.getParent());
        }

        @Override
        public Resource get(String resourcePath) {
            return JDBCResourceStore.this.get(path() + "/" + resourcePath);
        }

        @Override
        public List<Resource> list() {
            if (getType() != Type.DIRECTORY) {
                return Collections.EMPTY_LIST;
            }            

            List<Resource> list = new ArrayList<Resource>();

            for (JDBCDirectoryStructure.Entry child : entry.getChildren()) {
                list.add(new JDBCResource(child));
            }

            return list;

        }

        @Override
        public Type getType() {
            Boolean isDir = entry.isDirectory();
            return isDir == null ? Type.UNDEFINED : isDir ? Type.DIRECTORY : Type.RESOURCE ;
        }

        @Override
        public int hashCode() {
            return entry.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof JDBCResource)) {
                return false;
            }
            JDBCResource other = (JDBCResource) obj;
            return entry.equals(other.entry);
        }

        @Override
        public boolean delete() {
            List<Lock> locks = new ArrayList<Lock>();
            lockRecursively(locks);
            try {
                return entry.delete();
            } finally {
                for (Lock lock : locks) {
                    lock.release();
                }
            }
        }
        
        private void lockRecursively(List<Lock> locks) {
            for (JDBCDirectoryStructure.Entry child : entry.getChildren()) {
                new JDBCResource(child).lockRecursively(locks); 
            }
            locks.add(lock());            
        }

        @Override
        public boolean renameTo(Resource dest) {
            if (dest instanceof JDBCResource) {
                return entry.renameTo(((JDBCResource) dest).entry);
            } else {
                return Resources.renameByCopy(this, dest);
            }
        }

        @Override
        public Lock lock() {
            return lockProvider.acquire(entry.toString());
        }

        @Override
        public void addListener(ResourceListener listener) {
            //TODO: implement
        }

        @Override
        public void removeListener(ResourceListener listener) {
            //TODO: implement            
        }
        

        protected class CachingOutputStreamWrapper extends ProxyOutputStream {
            File tempFile;
            
            public CachingOutputStreamWrapper(File tempFile) throws IOException {
                super(new FileOutputStream(tempFile));
                this.tempFile = tempFile;
            }
            
            @Override
            public void close() throws IOException {            
                final Lock lock = lock();
                try {
                    entry.setContent(new FileInputStream(tempFile));
                } finally {
                    lock.release();
                }                
            }
        }
    }
}
