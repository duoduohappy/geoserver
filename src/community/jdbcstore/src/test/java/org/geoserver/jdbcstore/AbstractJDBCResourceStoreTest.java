package org.geoserver.jdbcstore;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.*;
import static org.geoserver.platform.resource.ResourceMatchers.*;

import java.io.InputStream;
import java.sql.ResultSet;

import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.geoserver.platform.resource.DataDirectoryResourceStore;
import org.geoserver.platform.resource.FileSystemResourceStore;
import org.geoserver.platform.resource.NullLockProvider;
import org.geoserver.platform.resource.Resource;
import org.geoserver.platform.resource.ResourceStore;
import org.junit.After;
import org.junit.Test;

public abstract class AbstractJDBCResourceStoreTest {
    
    DatabaseTestSupport support;
    
    @After
    public void cleanUp() throws Exception {
        support.close();
    }
    
    JDBCResourceStoreProperties getConfig(boolean enabled, boolean init) {
        JDBCResourceStoreProperties config = createMock(JDBCResourceStoreProperties.class);
        expect(config.isInitDb()).andStubReturn(init);
        expect(config.isEnabled()).andStubReturn(enabled);
        expect(config.isImport()).andStubReturn(init);
        expect(config.getIgnoreDirs()).andStubReturn(new String[] {"DirIgnore"});
        config.setInitDb(false);        
        expectLastCall();
        try {
            config.save();      
        } catch (Exception e) {}
        expectLastCall();
        support.stubConfig(config);
        replay(config);
        return config;
    }
    
    @Test
    public void testInitializeEmptyDB() throws Exception {
        JDBCResourceStoreProperties config = getConfig(true, true);
        
        @SuppressWarnings("unused")
        ResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        
        // Check that the database has a resource table with a root record
        
        ResultSet rs = support.getConnection().createStatement().executeQuery("SELECT * from resource where oid = 0");
        
        assertThat(rs.next(), describedAs("found root record",is(true)));
        assertThat(rs.getString("name"), equalTo(""));
        rs.getInt("parent");
        assertThat(rs.wasNull(), is(true));
        assertThat(rs.getBlob("content"), nullValue());
        assertThat(rs.next(), describedAs("only one root",is(false)));
    }
    
    void standardData() throws Exception {
        support.initialize();
        
        support.addFile("FileA", 0, "FileA Contents".getBytes());
        support.addFile("FileB", 0, "FileB Contents".getBytes());
        int c = support.addDir("DirC", 0);
        support.addFile("FileD", c, "FileD Contents".getBytes());
        support.addDir("DirE", 0);
        int f = support.addDir("DirF", c);
        int g = support.addDir("DirG", f);
        support.addFile("FileH", g, "FileH Contents".getBytes());
    }
    
    @Test
    public void testAcceptInitializedDB() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        {
            // Check that the database has a resource table with a root record
            
            ResultSet rs = support.getConnection().createStatement().executeQuery("SELECT * from resource where oid = 0");
            
            assertThat(rs.next(), describedAs("found root record",is(true)));
            assertThat(rs.getString("name"), equalTo(""));
            rs.getInt("parent");
            assertThat(rs.wasNull(), is(true));
            assertThat(rs.getBlob("content"), nullValue());
            assertThat(rs.next(), describedAs("only one root",is(false)));
        }
        {
            // Check that the database has one of the child nodes
            
            ResultSet rs = support.getConnection().createStatement().executeQuery("SELECT * from resource where parent = 0 and name='FileA'");
            
            assertThat(rs.next(), describedAs("found child FileA",is(true)));
            assertThat(rs.getString("name"), equalTo("FileA"));
            assertThat(rs.getInt("parent"), equalTo(0));
            assertThat(rs.wasNull(), is(false));
            assertThat(rs.getBinaryStream("content"), not(nullValue()));
            assertThat(rs.getInt("oid"), not(equalTo(0)));
        }
    }
    
    @Test
    public void testInitializeDatabaseWithIrrelevantTable() throws Exception {
        support.getConnection().createStatement().execute("CREATE TABLE foo (oid INTEGER PRIMARY KEY);");

        JDBCResourceStoreProperties config = getConfig(true, true);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        {
            // Check that the database has a resource table with a root record
            
            ResultSet rs = support.getConnection().createStatement().executeQuery("SELECT * from resource where oid = 0");
            
            assertThat(rs.next(), describedAs("found root record",is(true)));
            assertThat(rs.getString("name"), equalTo(""));
            rs.getInt("parent");
            assertThat(rs.wasNull(), is(true));
            assertThat(rs.getBlob("content"), nullValue());
            assertThat(rs.next(), describedAs("only one root",is(false)));
        }
        
    }
    
    @Test
    public void testBasicResourceQuery() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        
        Resource r = store.get("FileA");
        
        assertThat(r, not(nullValue()));
        
        assertThat(r, resource());
        
    }
    @Test
    public void testBasicDirectoryQuery() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        
        Resource r = store.get("DirE");
        
        assertThat(r, not(nullValue()));
        
        assertThat(r, directory());
        
    }
    
    @Test
    public void testBasicUndefinedQuery() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        
        Resource r = store.get("DoesntExist");
        
        assertThat(r, not(nullValue()));
        
        assertThat(r, undefined());
        
    }
    
    @Test
    public void testLongQuery() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        
        Resource r = store.get("DirC/DirF/DirG/FileH");
        
        assertThat(r, not(nullValue()));
        
        assertThat(r, resource());
        
    }
    @Test
    public void testBasicRead() throws Exception {
        standardData();
        
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        JDBCResourceStore store = new JDBCResourceStore(support.getDataSource(), config);
        store.setLockProvider(new NullLockProvider());
        
        Resource r = store.get("FileA");
        
        byte[] expected = "FileA Contents".getBytes();
        
        InputStream in = r.in();
        try {
            byte[] result = new byte[expected.length];
            assertThat(in.read(result), describedAs("file contents same length",equalTo(expected.length)));
            assertThat(result, equalTo(expected));
            assertThat(in.read(), describedAs("stream is empty",equalTo(-1)));
        } finally {
            in.close();
        }
        
    }
    
    @Test
    public void testIgnoreDir() throws Exception {
        JDBCResourceStoreProperties config = getConfig(true, false);
        
        ResourceStore dataDirStore = new DataDirectoryResourceStore();
        ResourceStore store = new JDBCResourceStore(support.getDataSource(), config,
                dataDirStore);
                
        assertEquals(store.get("DirIgnore"), dataDirStore.get("DirIgnore"));
        assertNotEquals(store.get("DirDontIgnore"), dataDirStore.get("DirDontIgnore"));
    }

}
