package org.geoserver.jdbcstore;

import org.junit.Before;
import org.junit.Ignore;

public class PostgresJDBCResourceStoreTest extends AbstractJDBCResourceStoreTest{
    
    @Before
    public void setUp() throws Exception {
        support = new PostgresTestSupport();
    }
    

}
