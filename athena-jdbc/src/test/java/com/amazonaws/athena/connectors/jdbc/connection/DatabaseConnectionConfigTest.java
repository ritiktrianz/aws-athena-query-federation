/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019-2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.connection;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseConnectionConfigTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_ENGINE = "mysql";
    private static final String TEST_JDBC_STRING = "jdbc:mysql://localhost:3306/testdb";
    private static final String TEST_SECRET = "testSecret";

    @Test
    public void testConstructorWithSecret()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        
        Assert.assertEquals(TEST_CATALOG, config.getCatalog());
        Assert.assertEquals(TEST_ENGINE, config.getEngine());
        Assert.assertEquals(TEST_JDBC_STRING, config.getJdbcConnectionString());
        Assert.assertEquals(TEST_SECRET, config.getSecret());
    }

    @Test
    public void testConstructorWithoutSecret()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING);
        
        Assert.assertEquals(TEST_CATALOG, config.getCatalog());
        Assert.assertEquals(TEST_ENGINE, config.getEngine());
        Assert.assertEquals(TEST_JDBC_STRING, config.getJdbcConnectionString());
        Assert.assertNull(config.getSecret());
    }

    @Test
    public void testEquals()
    {
        DatabaseConnectionConfig config1 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig config2 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig config3 = new DatabaseConnectionConfig(
            "differentCatalog", TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig configNoSecret1 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING);
        DatabaseConnectionConfig configNoSecret2 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING);

        Assert.assertEquals(config1, config2);
        Assert.assertEquals(config2, config1);
        Assert.assertEquals(configNoSecret1, configNoSecret2);

        Assert.assertNotEquals(config1, config3);
        Assert.assertNotEquals(config1, configNoSecret1);
        Assert.assertNotEquals(null, config1);
    }

    @Test
    public void testHashCode()
    {
        DatabaseConnectionConfig config1 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig config2 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig config3 = new DatabaseConnectionConfig(
            "differentCatalog", TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        
        Assert.assertEquals(config1.hashCode(), config2.hashCode());
        
        Assert.assertNotEquals(config1.hashCode(), config3.hashCode());
    }

    @Test
    public void testToString()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, TEST_SECRET);
        
        String toString = config.toString();
        
        Assert.assertTrue(toString.contains("DatabaseConnectionConfig"));
        Assert.assertTrue(toString.contains(TEST_CATALOG));
        Assert.assertTrue(toString.contains(TEST_ENGINE));
        Assert.assertTrue(toString.contains(TEST_JDBC_STRING));
        Assert.assertTrue(toString.contains(TEST_SECRET));
    }

    @Test
    public void testToStringWithoutSecret()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING);
        
        String toString = config.toString();
        
        Assert.assertTrue(toString.contains("DatabaseConnectionConfig"));
        Assert.assertTrue(toString.contains(TEST_CATALOG));
        Assert.assertTrue(toString.contains(TEST_ENGINE));
        Assert.assertTrue(toString.contains(TEST_JDBC_STRING));
        Assert.assertTrue(toString.contains("secret='null'"));
    }

    @Test
    public void testEqualsWithDifferentEngines()
    {
        DatabaseConnectionConfig mysqlConfig = new DatabaseConnectionConfig(
            TEST_CATALOG, "mysql", TEST_JDBC_STRING, TEST_SECRET);
        DatabaseConnectionConfig postgresConfig = new DatabaseConnectionConfig(
            TEST_CATALOG, "postgresql", TEST_JDBC_STRING, TEST_SECRET);
        
        Assert.assertNotEquals(mysqlConfig, postgresConfig);
        Assert.assertNotEquals(mysqlConfig.hashCode(), postgresConfig.hashCode());
    }

    @Test
    public void testEqualsWithDifferentJdbcStrings()
    {
        DatabaseConnectionConfig config1 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, "jdbc:mysql://host1:3306/db", TEST_SECRET);
        DatabaseConnectionConfig config2 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, "jdbc:mysql://host2:3306/db", TEST_SECRET);
        
        Assert.assertNotEquals(config1, config2);
    }

    @Test
    public void testEqualsWithDifferentSecrets()
    {
        DatabaseConnectionConfig config1 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, "secret1");
        DatabaseConnectionConfig config2 = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_STRING, "secret2");
        
        Assert.assertNotEquals(config1, config2);
    }
}
