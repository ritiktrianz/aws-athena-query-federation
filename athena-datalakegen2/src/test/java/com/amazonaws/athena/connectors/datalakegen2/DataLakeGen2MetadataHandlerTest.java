/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.datalakegen2.resolver.DataLakeGen2CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2MetadataHandler.PARTITION_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DataLakeGen2MetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DataLakeGen2MetadataHandlerTest.class);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", DataLakeGen2Constants.NAME,
    		  "datalakegentwo://jdbc:sqlserver://hostname;databaseName=fakedatabase");
    private DataLakeGen2MetadataHandler dataLakeGen2MetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class, RETURNS_DEEP_STUBS);
        this.connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        logger.info(" this.connection.."+ this.connection);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = mock(SecretsManagerClient.class);
        this.athena = mock(AthenaClient.class);
        when(this.secretsManager.getSecretValue(eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.dataLakeGen2MetadataHandler = new DataLakeGen2MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME));
        this.federatedIdentity = mock(FederatedIdentity.class);
    }

    @Test
    public void getPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.dataLakeGen2MetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }

        assertEquals(Collections.singletonList("[partition_number : 0]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();

        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
        when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        DataLakeGen2MetadataHandler dataLakeGen2MetadataHandler = new DataLakeGen2MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME));

        dataLakeGen2MetadataHandler.doGetTableLayout(mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.dataLakeGen2MetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.dataLakeGen2MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(PARTITION_NUMBER, "0"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");
        GetTableResponse getTableResponse = this.dataLakeGen2MetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doListTables() throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String schemaName = "TESTSCHEMA";
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, "queryId", "testCatalog", schemaName, null, 3);

        DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockDatabaseMetaData.getTables(any(), any(), any(), any())).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(3)).thenReturn("TESTTABLE").thenReturn("testtable").thenReturn("testTABLE");
        when(mockResultSet.getString(2)).thenReturn(schemaName);

        mockStatic(JDBCUtil.class);
        when(JDBCUtil.getSchemaTableName(mockResultSet)).thenReturn(new TableName("TESTSCHEMA", "TESTTABLE"))
                .thenReturn(new TableName("TESTSCHEMA", "testtable"))
                .thenReturn(new TableName("TESTSCHEMA", "testTABLE"));

        when(this.jdbcConnectionFactory.getConnection(any())).thenReturn(connection);

        ListTablesResponse listTablesResponse = this.dataLakeGen2MetadataHandler.doListTables(blockAllocator, listTablesRequest);

        TableName[] expectedTables = {
                new TableName("TESTSCHEMA", "TESTTABLE"),
                new TableName("TESTSCHEMA", "testTABLE"),
                new TableName("TESTSCHEMA", "testtable")
        };

        assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        BlockAllocator allocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", "testCatalog");

        GetDataSourceCapabilitiesResponse response =
                dataLakeGen2MetadataHandler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals("testCatalog", response.getCatalogName());

        // Verify filter pushdown capabilities
        List<OptimizationSubType> filterPushdown = capabilities.get("supports_filter_pushdown");
        assertNotNull("Expected supports_filter_pushdown capability to be present", filterPushdown);
        assertEquals(2, filterPushdown.size());
        assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("sorted_range_set")));
        assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("nullable_comparison")));

        // Verify complex expression pushdown capabilities
        List<OptimizationSubType> complexPushdown = capabilities.get("supports_complex_expression_pushdown");
        assertNotNull("Expected supports_complex_expression_pushdown capability to be present", complexPushdown);
        assertEquals(1, complexPushdown.size());
        OptimizationSubType complexSubType = complexPushdown.get(0);
        assertEquals("supported_function_expression_types", complexSubType.getSubType());
        assertNotNull("Expected function expression types to be present", complexSubType.getProperties());
        assertFalse("Expected function expression types to be non-empty", complexSubType.getProperties().isEmpty());

        // Verify top-n pushdown capabilities
        List<OptimizationSubType> topNPushdown = capabilities.get("supports_top_n_pushdown");
        assertNotNull("Expected supports_top_n_pushdown capability to be present", topNPushdown);
        assertEquals(1, topNPushdown.size());
        assertEquals("SUPPORTS_ORDER_BY", topNPushdown.get(0).getSubType());
    }

    @Test
    public void testConvertDatasourceTypeToArrowWithDataLakeGen2Types()
    {
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        Map<String, String> configOptions = new HashMap<>();
        int precision = 0;

        Map<String, ArrowType> expectedMappings = new HashMap<>();
        expectedMappings.put("BIT", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType());
        expectedMappings.put("TINYINT", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType());
        expectedMappings.put("NUMERIC", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType());
        expectedMappings.put("SMALLMONEY", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType());
        expectedMappings.put("DATE", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType());
        expectedMappings.put("DATETIME", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
        expectedMappings.put("DATETIME2", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
        expectedMappings.put("SMALLDATETIME", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
        expectedMappings.put("DATETIMEOFFSET", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());

        int index = 1;
        for (Map.Entry<String, ArrowType> entry : expectedMappings.entrySet()) {
            String dataLakeType = entry.getKey();
            ArrowType expectedArrowType = entry.getValue();

            try {
                when(metaData.getColumnTypeName(index)).thenReturn(dataLakeType);

                Optional<ArrowType> actual = dataLakeGen2MetadataHandler.convertDatasourceTypeToArrow(index, precision, configOptions, metaData);
                assertTrue("Optional value is empty for type " + dataLakeType, actual.isPresent());
                assertEquals("Failed for type " + dataLakeType, expectedArrowType, actual.get());

                index++;
            } catch (SQLException e) {
                fail("SQLException occurred while testing type " + dataLakeType + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testGetSchemaWithCustomDataTypeQuery()
    {
        try {
            BlockAllocator blockAllocator = new BlockAllocatorImpl();
            TableName tableName = new TableName("testSchema", "testTable");

            String[] columnSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
            Object[][] columnValues = {
                    {Types.INTEGER, 12, "testCol1", 0, 0},
                    {Types.VARCHAR, 25, "testCol2", 0, 0}
            };
            ResultSet columnResultSet = mockResultSet(columnSchema, columnValues, new AtomicInteger(-1));
            when(connection.getMetaData().getColumns(any(), any(), any(), any())).thenReturn(columnResultSet);

            PreparedStatement mockStmt = mock(PreparedStatement.class);
            when(connection.prepareStatement(any())).thenReturn(mockStmt);

            String[] dataTypeSchema = {"COLUMN_NAME", "DATA_TYPE"};
            Object[][] dataTypeValues = {
                    {"testCol1", "int"},
                    {"testCol2", "varchar"}
            };
            ResultSet dataTypeResultSet = mockResultSet(dataTypeSchema, dataTypeValues, new AtomicInteger(-1));
            when(mockStmt.executeQuery()).thenReturn(dataTypeResultSet);

            when(connection.getCatalog()).thenReturn("testCatalog");

            GetTableResponse response = dataLakeGen2MetadataHandler.doGetTable(
                    blockAllocator,
                    new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap())
            );

            Schema schema = response.getSchema();
            assertEquals(3, schema.getFields().size()); // 2 columns + partition column
            assertEquals("testCol1", schema.getFields().get(0).getName());
            assertEquals("testCol2", schema.getFields().get(1).getName());
            assertEquals(PARTITION_NUMBER, schema.getFields().get(2).getName());
        } catch (Exception e) {
            fail("Test failed due to exception: " + e.getMessage());
        }
    }

    @Test
    public void testDoGetSplitsWithQueryPassthrough()
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        TableName tableName = new TableName("testSchema", "testTable");

        // Create constraints with query passthrough enabled
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);

        // Mock query passthrough arguments
        Map<String, String> queryPassthroughArguments = new HashMap<>();
        queryPassthroughArguments.put("queryPassthrough", "true");
        when(constraints.getQueryPassthroughArguments()).thenReturn(queryPassthroughArguments);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                federatedIdentity,
                "testQueryId",
                "testCatalog",
                tableName,
                mock(Block.class),
                Collections.emptyList(),
                constraints,
                null
        );

        GetSplitsResponse getSplitsResponse = dataLakeGen2MetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        // Verify the response
        assertNotNull(getSplitsResponse);
        assertEquals(1, getSplitsResponse.getSplits().size());
        Split split = getSplitsResponse.getSplits().iterator().next();
        Map<String, String> properties = split.getProperties();
        assertTrue("Split properties should contain query passthrough indicator", properties.containsKey("queryPassthrough"));
        assertEquals("true", properties.get("queryPassthrough"));
    }


    @Test(expected = RuntimeException.class)
    public void testGetSchemaWithDataTypeQueryError()
    {
        try {
            TableName tableName = new TableName("testSchema", "testTable");

            String[] columnSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
            Object[][] columnValues = {
                    {Types.INTEGER, 12, "testCol1", 0, 0}
            };
            ResultSet columnResultSet = mockResultSet(columnSchema, columnValues, new AtomicInteger(-1));
            when(connection.getMetaData().getColumns(any(), any(), any(), any())).thenReturn(columnResultSet);

            when(connection.prepareStatement(any())).thenThrow(new RuntimeException(new SQLException("Test error")));

            dataLakeGen2MetadataHandler.doGetTable(
                    new BlockAllocatorImpl(),
                    new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap())
            );
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }
}
