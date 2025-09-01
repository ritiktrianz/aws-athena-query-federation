/*-
 * #%L
 * athena-datalakegen2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataLakeGen2JdbcConnectionFactoryTest
{
    private static final String JDBC_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final int JDBC_DEFAULT_PORT = 1433;

    @Test
    public void testGetConnectionWithCredentialsReplacesSecret()
    {
        try {
            DefaultCredentials credentials = new DefaultCredentials("testuser", "testpass");
            CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

            String originalJdbcUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb;${secret}";
            DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", DataLakeGen2Constants.NAME, originalJdbcUrl, "secret");

            DatabaseConnectionInfo info = new DatabaseConnectionInfo(JDBC_DRIVER_CLASS, JDBC_DEFAULT_PORT);

            Driver mockDriver = mock(Driver.class);
            Connection mockConnection = mock(Connection.class);
            AtomicReference<String> actualUrl = new AtomicReference<>();

            when(mockDriver.acceptsURL(any())).thenReturn(true);
            when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
                actualUrl.set(invocation.getArgument(0));
                return mockConnection;
            });

            DriverManager.registerDriver(mockDriver);

            DataLakeGen2JdbcConnectionFactory factory = new DataLakeGen2JdbcConnectionFactory(config, new HashMap<>(), info);
            Connection connection = factory.getConnection(credentialsProvider);

            String expectedUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb;user=testuser;password=testpass";
            assertEquals(expectedUrl, actualUrl.get());
            assertEquals(mockConnection, connection);

            DriverManager.deregisterDriver(mockDriver);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetConnectionWithoutCredentials()
    {
        try {
            String jdbcUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb;user=admin;password=admin";
            DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", DataLakeGen2Constants.NAME, jdbcUrl, "secret");

            DatabaseConnectionInfo info = new DatabaseConnectionInfo(JDBC_DRIVER_CLASS, JDBC_DEFAULT_PORT);

            Driver mockDriver = mock(Driver.class);
            Connection mockConnection = mock(Connection.class);
            AtomicReference<String> actualUrl = new AtomicReference<>();

            when(mockDriver.acceptsURL(any())).thenReturn(true);
            when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
                actualUrl.set(invocation.getArgument(0));
                return mockConnection;
            });

            DriverManager.registerDriver(mockDriver);

            DataLakeGen2JdbcConnectionFactory factory = new DataLakeGen2JdbcConnectionFactory(config, new HashMap<>(), info);
            Connection connection = factory.getConnection(null);

            assertEquals(jdbcUrl, actualUrl.get());
            assertEquals(mockConnection, connection);

            DriverManager.deregisterDriver(mockDriver);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnectionWithInvalidDriver()
    {
        String jdbcUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb";
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", DataLakeGen2Constants.NAME, jdbcUrl, "secret");

        DatabaseConnectionInfo info = new DatabaseConnectionInfo("invalid.driver.class", JDBC_DEFAULT_PORT);

        DataLakeGen2JdbcConnectionFactory factory = new DataLakeGen2JdbcConnectionFactory(config, new HashMap<>(), info);
        factory.getConnection(null);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnectionWhenDriverFailsToConnect() throws Exception
    {
        DefaultCredentials credentials = new DefaultCredentials("testuser", "testpass");
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

        String jdbcUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb";
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", DataLakeGen2Constants.NAME, jdbcUrl, "secret");

        DatabaseConnectionInfo info = new DatabaseConnectionInfo(JDBC_DRIVER_CLASS, JDBC_DEFAULT_PORT);

        Driver mockDriver = mock(Driver.class);
        when(mockDriver.acceptsURL(anyString())).thenReturn(true);
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenThrow(new SQLException("DB error", "08001", 999));

        DriverManager.registerDriver(mockDriver);

        try {
            DataLakeGen2JdbcConnectionFactory factory = new DataLakeGen2JdbcConnectionFactory(config, new HashMap<>(), info);
            factory.getConnection(credentialsProvider);
        }
        finally {
            DriverManager.deregisterDriver(mockDriver);
        }
    }

    @Test
    public void testGetConnectionWithCustomProperties()
    {
        try {
            String jdbcUrl = "jdbc:sqlserver://test.database.windows.net:1433;databaseName=testdb";
            DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", DataLakeGen2Constants.NAME, jdbcUrl, "secret");

            DatabaseConnectionInfo info = new DatabaseConnectionInfo(JDBC_DRIVER_CLASS, JDBC_DEFAULT_PORT);

            Map<String, String> customProps = new HashMap<>();
            customProps.put("connectTimeout", "30");
            customProps.put("socketTimeout", "30");

            Driver mockDriver = mock(Driver.class);
            Connection mockConnection = mock(Connection.class);
            AtomicReference<Properties> actualProps = new AtomicReference<>();

            when(mockDriver.acceptsURL(any())).thenReturn(true);
            when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
                actualProps.set(invocation.getArgument(1));
                return mockConnection;
            });

            DriverManager.registerDriver(mockDriver);

            DataLakeGen2JdbcConnectionFactory factory = new DataLakeGen2JdbcConnectionFactory(config, customProps, info);
            Connection connection = factory.getConnection(null);

            Properties props = actualProps.get();
            assertEquals("30", props.getProperty("connectTimeout"));
            assertEquals("30", props.getProperty("socketTimeout"));
            assertEquals(mockConnection, connection);

            DriverManager.deregisterDriver(mockDriver);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}