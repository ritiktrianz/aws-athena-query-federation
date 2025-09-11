/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory.SECRET_NAME_PATTERN;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class GenericJdbcConnectionFactoryTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_ENGINE = "mysql";
    private static final String TEST_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String TEST_JDBC_URL = "jdbc:mysql://localhost:3306/testdb";
    private static final String TEST_JDBC_URL_WITH_SECRET = "jdbc:mysql://localhost:3306/testdb?user=${testSecret}&password=${testSecret2}";
    private static final int TEST_DEFAULT_PORT = 3306;

    @Mock
    private CredentialsProvider mockCredentialsProvider;

    private DatabaseConnectionConfig databaseConnectionConfig;
    private DatabaseConnectionInfo databaseConnectionInfo;
    private GenericJdbcConnectionFactory connectionFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        databaseConnectionInfo = new DatabaseConnectionInfo(TEST_DRIVER_CLASS, TEST_DEFAULT_PORT);
        databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, TEST_JDBC_URL, "testSecret");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("testProp", "testValue");
        
        connectionFactory = new GenericJdbcConnectionFactory(databaseConnectionConfig, properties, databaseConnectionInfo);
    }

    @Test
    public void matchSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertTrue(secretMatcher.find());
    }

    @Test
    public void matchIncorrectSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=*_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertFalse(secretMatcher.find());
    }

    @Test 
    public void testMultipleSecretPatterns()
    {
        String jdbcString = "mysql://jdbc:mysql://${hostSecret}:3306/${dbSecret}?user=${userSecret}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcString);
        
        int matchCount = 0;
        while (secretMatcher.find()) {
            matchCount++;
        }
        
        Assert.assertEquals(3, matchCount);
    }

    @Test
    public void testSecretPatternWithSpecialCharacters()
    {
        String jdbcString = "mysql://jdbc:mysql://localhost:3306/test?${aws:secret:rds/prod/credentials}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcString);
        
        Assert.assertTrue(secretMatcher.find());
    }

    @Test
    public void testValidSecretPatternEdgeCases()
    {
        String[] validPatterns = {
            "${simple}",
            "${with_underscore}",
            "${with-dash}",
            "${with.dot}",
            "${with:colon}",
            "${with/slash}",
            "${with+plus}",
            "${with=equals}",
            "${with@at}",
            "${with!exclamation}"
        };
        
        for (String pattern : validPatterns) {
            Matcher matcher = SECRET_NAME_PATTERN.matcher(pattern);
            Assert.assertTrue("Pattern should match: " + pattern, matcher.find());
        }
    }

    @Test
    public void testGetConnectionWithNullCredentialsProvider() {

        Exception exception = assertThrows(Exception.class, () -> connectionFactory.getConnection(null));
        
        assertTrue("Exception should be related to driver loading",
            exception.getMessage().contains("Failed to load driver class") ||
            exception.getCause() instanceof ClassNotFoundException);
    }

    @Test
    public void testGetConnectionWithSecretReplacement() {
        DatabaseConnectionConfig configWithSecrets = new DatabaseConnectionConfig(
            TEST_CATALOG, TEST_ENGINE, TEST_JDBC_URL_WITH_SECRET, "testSecret");
        GenericJdbcConnectionFactory factoryWithSecrets = new GenericJdbcConnectionFactory(
            configWithSecrets, new HashMap<>(), databaseConnectionInfo);

        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put("user", "testUser");
        credentialMap.put("password", "testPassword");
        
        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Exception exception = assertThrows(Exception.class, () -> factoryWithSecrets.getConnection(mockCredentialsProvider));
        
        assertTrue("Exception should be related to driver loading",
            exception.getMessage().contains("Failed to load driver class") ||
            exception.getCause() instanceof ClassNotFoundException);
    }

    @Test
    public void testGetConnectionWithInvalidDriver() {
        // Create factory with invalid driver
        DatabaseConnectionInfo invalidDriverInfo = new DatabaseConnectionInfo("invalid.driver.Class", TEST_DEFAULT_PORT);
        GenericJdbcConnectionFactory invalidFactory = new GenericJdbcConnectionFactory(
            databaseConnectionConfig, new HashMap<>(), invalidDriverInfo);

        // Test that appropriate exception is thrown
        Exception exception = assertThrows(Exception.class, () -> invalidFactory.getConnection(null));

        assertTrue("Exception should be related to driver", 
            exception.getMessage().contains("invalid.driver.Class") || 
            exception.getCause() instanceof ClassNotFoundException);
    }

    @Test
    public void testConstructorWithNullParameters() {
        assertThrows("Should throw exception for null databaseConnectionInfo",
            NullPointerException.class, () -> new GenericJdbcConnectionFactory(databaseConnectionConfig, new HashMap<>(), null));

        assertThrows("Should throw exception for null databaseConnectionConfig", 
            NullPointerException.class, () -> new GenericJdbcConnectionFactory(null, new HashMap<>(), databaseConnectionInfo));
    }

}
