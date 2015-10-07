package org.geoserver.jdbcstore;

import org.junit.Before;

public class PostgresJDBCResourceStoreTest extends AbstractJDBCResourceStoreTest{
    
    @Before
    public void setUp() throws Exception {
        support = new PostgresTestSupport();
    }
    

}
