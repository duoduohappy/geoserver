package org.geoserver.jdbcstore;

import static org.easymock.classextension.EasyMock.*;

import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.junit.Before;

public class PostgresJDBCResourceTheoryTest extends AbstractJDBCResourceTheoryTest {

    JDBCResourceStore store;

    @Override
    protected JDBCResourceStore getStore() {
        return store;
    }
       
    @Before
    public void setUp() throws Exception {
        support = new PostgresTestSupport();
        
        standardData();
        
        JDBCResourceStoreProperties config = mockConfig(true, false);
        replay(config);
        
        store = new JDBCResourceStore(support.getDataSource(), config);
    }
}
