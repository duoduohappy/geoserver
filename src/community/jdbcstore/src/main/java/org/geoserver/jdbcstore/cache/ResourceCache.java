package org.geoserver.jdbcstore.cache;

import java.io.File;
import java.io.IOException;

import org.geoserver.platform.resource.Resource;

public interface ResourceCache {
    public File cache(Resource res, boolean createDirectory) throws IOException;
}
