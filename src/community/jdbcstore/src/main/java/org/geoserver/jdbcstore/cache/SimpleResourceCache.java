package org.geoserver.jdbcstore.cache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.geoserver.platform.resource.Resource;
import org.geoserver.platform.resource.Resource.Type;

public class SimpleResourceCache implements ResourceCache {
    File base;
    boolean cacheChildren;
    
    public SimpleResourceCache() {
        this(false);
    }
    
    public SimpleResourceCache(boolean cacheChildren) {
        this.cacheChildren = cacheChildren;
    }
        
    public SimpleResourceCache(File base) {
        this.base=base;
    }
    
    public File getBase() {
        return base;
    }

    public void setBase(File base) {
        this.base = base;
    }


    void cacheData(Resource res, File file) throws IOException {
        assert res.getType()==Type.RESOURCE;
        OutputStream out = new FileOutputStream(file);
        try {
            InputStream in = res.in();
            try {
                IOUtils.copy(in, out);
            } finally {
                in.close();
            }
        } finally {
            out.close();
        }
        
    }
    
    void cacheChildren(Resource res, File file) throws IOException {
        assert res.getType()==Type.DIRECTORY;
        
        for (Resource child : res.list()) {
            cache(child, false);
        };
    }
    
    @Override
    public File cache(Resource res, boolean createDirectory) throws IOException {
        String path = res.path();
        long mtime = res.lastmodified();
        File cached = new File(base, path);
        if(!cached.exists() || cached.lastModified()>mtime) {
            Resource.Type type = res.getType();
            switch (type) {
            case RESOURCE:
                cached.getParentFile().mkdirs();
                cacheData(res, cached);
                break;
            case DIRECTORY:
                cached.mkdirs();
                if (cacheChildren) {
                    cacheChildren(res, cached);
                }
                break;
            case UNDEFINED:
                if (createDirectory) {
                    cached.mkdirs();
                } else {
                    cached.getParentFile().mkdirs();
                }
                break;
            }
        }
        return cached;
    }
}
