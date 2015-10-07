package org.geoserver.jdbcstore;

import javax.sql.DataSource;

import org.geoserver.jdbcstore.cache.ResourceCache;
import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.geoserver.platform.resource.ResourceStore;
import org.springframework.beans.factory.FactoryBean;

public class JDBCResourceStoreFactoryBean implements FactoryBean<ResourceStore> {
    
    private ResourceStore resourceStore;
    
    public JDBCResourceStoreFactoryBean(ResourceStore fallbackStore, DataSource ds, 
            JDBCResourceStoreProperties config) {
        if (config.isEnabled()) {
            resourceStore = new JDBCResourceStore(ds, config, fallbackStore);
        } else {
            resourceStore = fallbackStore;
        }
    }
    
    public void setCache(ResourceCache cache) {
        if (resourceStore instanceof JDBCResourceStore) {
            ((JDBCResourceStore) resourceStore).setCache(cache);
        }
    }

    @Override
    public ResourceStore getObject() throws Exception {
        return resourceStore;
    }

    @Override
    public Class<?> getObjectType() {
        return ResourceStore.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
