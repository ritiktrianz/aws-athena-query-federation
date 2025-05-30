package com.amazonaws.athena.connectors.cloudera;/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.*;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class ImpalaJdbcConnectionFactoryTest {
    @Test(expected = RuntimeException.class)
    public void getConnectionTest() throws ClassNotFoundException, SQLException {
        DefaultCredentials expectedCredential = new DefaultCredentials("impala", "impala");
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(expectedCredential);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", ImpalaConstants.IMPALA_NAME,
                "impala://jdbc:impala://23.21.178.97:10000/athena;AuthMech=3;UID=hive;PWD=''", "impala");
        Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
        DatabaseConnectionInfo DatabaseConnectionInfo = new DatabaseConnectionInfo(ImpalaConstants.IMPALA_DRIVER_CLASS, ImpalaConstants.IMPALA_DEFAULT_PORT);
        Connection connection =  new ImpalaJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES,DatabaseConnectionInfo).getConnection(credentialsProvider);
        String originalURL = connection.getMetaData().getURL();
        Driver drv = DriverManager.getDriver(originalURL);
        String driverClass = drv.getClass().getName();
        Assert.assertEquals("com.cloudera.impala.jdbc.Driver", driverClass);
    }
}
