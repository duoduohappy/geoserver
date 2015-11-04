package org.geoserver.jdbcstore;

import static org.easymock.classextension.EasyMock.*;
import static org.hamcrest.Matchers.describedAs;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.geoserver.jdbcconfig.internal.Util;
import org.geoserver.jdbcstore.internal.JDBCResourceStoreProperties;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.google.common.base.Optional;

public class H2TestSupport implements DatabaseTestSupport {
    
    JDBCResourceStore store;
    JdbcDataSource ds;
    Connection conn;
    PreparedStatement insert;
    
    public H2TestSupport() throws Exception {
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test");
        conn = ds.getConnection();
    }
    
    @Override
    public void stubConfig(JDBCResourceStoreProperties config) {
        expect(config.getInitScript()).andStubReturn(JDBCResourceStoreProperties.class.getResource("init.h2.sql"));
        expect(config.getJdbcUrl()).andStubReturn(Optional.of("jdbc:h2:mem:test"));
        expect(config.getJndiName()).andStubReturn(Optional.<String>absent());
        expect(config.getProperty(eq("driverClassName"))).andStubReturn("org.h2.Driver");
        expect(config.getProperty(eq("driverClassName"), (String)anyObject())).andStubReturn("org.h2.Driver");
    }
    
    @Override
    public void initialize() throws Exception  {
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(ds);
        
        Util.runScript(JDBCResourceStoreProperties.class.getResource("init.h2.sql"), template.getJdbcOperations(), null);

    }
    
    private PreparedStatement getInsert() throws SQLException {
        if(insert==null) {
            insert = conn.prepareStatement("INSERT INTO resource (name, parent, content) VALUES (?, ?, ?)");
        }
        return insert;
    }
    
    @Override
    public int addFile(String name, int parent, byte[] content) throws SQLException  {
        getInsert().setString(1, name);
        getInsert().setInt(2, parent);
        getInsert().setBytes(3, content);
        getInsert().execute();
        ResultSet rs = getInsert().getGeneratedKeys();
        if(rs.next()) {
            return rs.getInt(1);
        } else {
            throw new IllegalStateException("Could not add test file "+name);
        }
    }
    
    @Override
    public int addDir(String name, int parent) throws SQLException  {
        getInsert().setString(1, name);
        getInsert().setInt(2, parent);
        getInsert().setBytes(3, null);
        getInsert().execute();
        ResultSet rs = getInsert().getGeneratedKeys();
        if(rs.next()) {
            return rs.getInt(1);
        } else {
            throw new IllegalStateException("Could not add test directory "+name);
        }
    }
    
    @Override
    public int getRoot() {
        return 0;
    }
    
    @Override
    public DataSource getDataSource() {
        return ds;
    }
    
    @Override
    public Connection getConnection() throws SQLException { 
        return conn;
    }

    @Override
    public void close() throws SQLException {
        conn.close();
        
        // Verify that all the connections are closed by opening a new one and checking if the 
        // database is empty.
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test");
        conn = ds.getConnection();
        try {
            ResultSet rs = conn.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
            
            boolean result = false;
            while(rs.next()) {
                result=true;
                System.out.printf("%s\n", rs.getString("TABLE_NAME"));
            }
            assertThat(result, describedAs("connection closed", is(false)));
        } finally {
            conn.close();
        }
    }

}
