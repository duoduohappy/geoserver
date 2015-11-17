package org.geoserver.jdbcstore;

import static org.easymock.classextension.EasyMock.*;

import org.geoserver.jdbcstore.cache.SimpleResourceCache;
import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.geoserver.platform.resource.NullLockProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class H2JDBCResourceTheoryTest extends AbstractJDBCResourceTheoryTest {

    JDBCResourceStore store;
    
    @Override
    protected JDBCResourceStore getStore() {
        return store;
    }
    
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();
    
    @Before
    public void setUp() throws Exception {
        support = new H2TestSupport();
        
        standardData();
        
        JDBCResourceStoreProperties config = mockConfig(true, false);
        replay(config);
        
        store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        store.setCache(new SimpleResourceCache(folder.getRoot()));
    }

}
