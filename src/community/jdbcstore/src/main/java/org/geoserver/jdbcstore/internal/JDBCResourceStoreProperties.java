/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.jdbcstore.internal;

import org.geoserver.jdbcloader.JDBCLoaderProperties;

/**
 * Configuration information for JDBCResourceStore
 * 
 * @author Kevin Smith, Boundless
 *
 */
public class JDBCResourceStoreProperties extends JDBCLoaderProperties {

    private static final long serialVersionUID = -3335880912330668027L;
    
    public JDBCResourceStoreProperties(JDBCResourceStorePropertiesFactoryBean factory) {
        super(factory);
    }
    
    //jdbcstore specific properties may go here.
    
    public boolean isDeleteDestinationOnRename() {
        return Boolean.valueOf(getProperty("deleteDestinationOnRename", "false"));
    }
}