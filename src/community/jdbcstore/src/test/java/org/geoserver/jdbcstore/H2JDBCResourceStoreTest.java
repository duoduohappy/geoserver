package org.geoserver.jdbcstore;

import org.junit.Before;

public class H2JDBCResourceStoreTest extends AbstractJDBCResourceStoreTest {
    
    @Before
    public void setUp() throws Exception {
        support = new H2TestSupport();
    }
    
}
