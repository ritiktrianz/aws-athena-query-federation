/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OracleQueryStringBuilderTest
{
    private static final String QUOTE_CHARACTER = "\"";
    private static final String TEST_CATALOG = "TEST_CATALOG";
    private static final String TEST_SCHEMA = "TEST_SCHEMA";
    private static final String TEST_TABLE = "TEST_TABLE";
    private static final String TEST_PARTITION = "TEST_PARTITION";

    @Mock
    private FederationExpressionParser mockFederationExpressionParser;

    @Mock
    private Split mockSplit;

    private OracleQueryStringBuilder queryStringBuilder;

    @Before
    public void setUp()
    {
        queryStringBuilder = new OracleQueryStringBuilder(QUOTE_CHARACTER, mockFederationExpressionParser);
    }

    @Test
    public void testConstructor()
    {
        assertNotNull("QueryStringBuilder should not be null", queryStringBuilder);
    }

    @Test
    public void testGetFromClauseWithSplit_WithCatalogSchemaAndTable()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\".\"%s\" PARTITION (%s) ", 
                                       TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_PARTITION);
        assertEquals("From clause should match expected format", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithSchemaAndTableOnly()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(null, TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\" PARTITION (%s) ", 
                                       TEST_SCHEMA, TEST_TABLE, TEST_PARTITION);
        assertEquals("From clause should match expected format", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithTableOnly()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(null, null, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\" PARTITION (%s) ", TEST_TABLE, TEST_PARTITION);
        assertEquals("From clause should match expected format", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithAllPartitions()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, OracleMetadataHandler.ALL_PARTITIONS);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(OracleMetadataHandler.ALL_PARTITIONS);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\".\"%s\" ", 
                                       TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        assertEquals("From clause should not include partition for ALL_PARTITIONS", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithEmptyCatalog()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit("", TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\" PARTITION (%s) ", 
                                       TEST_SCHEMA, TEST_TABLE, TEST_PARTITION);
        assertEquals("From clause should not include empty catalog", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithEmptySchema()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(TEST_CATALOG, "", TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\" PARTITION (%s) ", 
                                       TEST_CATALOG, TEST_TABLE, TEST_PARTITION);
        assertEquals("From clause should not include empty schema", expected, result);
    }

    @Test
    public void testGetFromClauseWithSplit_WithSpecialCharactersInPartition()
    {
        // Setup
        String specialPartition = "PART_2024_01_01";
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, specialPartition);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(specialPartition);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        String expected = String.format(" FROM \"%s\".\"%s\".\"%s\" PARTITION (%s) ", 
                                       TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, specialPartition);
        assertEquals("From clause should handle special characters in partition", expected, result);
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        // Execute
        List<String> result = queryStringBuilder.getPartitionWhereClauses(mockSplit);

        // Verify
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty list", result.isEmpty());
        assertEquals("Result should be empty list", Collections.emptyList(), result);
    }


    @Test
    public void testQuoteCharacterUsage()
    {
        // Setup
        Map<String, String> properties = new HashMap<>();
        properties.put(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        when(mockSplit.getProperties()).thenReturn(properties);
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TEST_PARTITION);

        // Execute
        String result = queryStringBuilder.getFromClauseWithSplit(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, mockSplit);

        // Verify
        assertTrue("Result should contain quoted catalog", result.contains("\"" + TEST_CATALOG + "\""));
        assertTrue("Result should contain quoted schema", result.contains("\"" + TEST_SCHEMA + "\""));
        assertTrue("Result should contain quoted table", result.contains("\"" + TEST_TABLE + "\""));
    }
} 