package org.geoserver.jdbcstore.cache;

import java.io.File;

import javax.servlet.ServletContext;

import org.geoserver.platform.GeoServerResourceLoader;
import org.springframework.web.context.ServletContextAware;

public class DataDirectoryResourceCache extends SimpleResourceCache implements ServletContextAware {

    public DataDirectoryResourceCache() {}
    
    @Override
    public void setServletContext(ServletContext servletContext) {
        String data = GeoServerResourceLoader.lookupGeoServerDataDirectory(servletContext);
        if (data != null) {
            this.base = new File(data);
        } else {
            throw new IllegalStateException("Unable to determine data directory");
        }
    }

}
