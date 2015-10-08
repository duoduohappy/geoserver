package org.geoserver.jdbcstore;

import static org.easymock.classextension.EasyMock.*;

import org.geoserver.jdbcstore.cache.SimpleResourceCache;
import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class PostgresJDBCResourceTheoryTest extends AbstractJDBCResourceTheoryTest {

    JDBCResourceStore store;

    @Override
    protected JDBCResourceStore getStore() {
        return store;
    }
    
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();
       
    @Before
    public void setUp() throws Exception {
        support = new PostgresTestSupport();
        
        standardData();
        
        JDBCResourceStoreProperties config = mockConfig(true, false);
        replay(config);
        
        store = new JDBCResourceStore(support.getDataSource(), config);
        store.setCache(new SimpleResourceCache(folder.getRoot()));
    }
}
