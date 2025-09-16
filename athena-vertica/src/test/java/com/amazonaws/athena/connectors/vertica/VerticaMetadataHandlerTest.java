/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import com.amazonaws.athena.connectors.vertica.query.VerticaExportQueryBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;

@RunWith(MockitoJUnitRunner.class)

public class VerticaMetadataHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandlerTest.class);
    private static final String[] TABLE_TYPES = new String[]{"TABLE"};

    private static final String PREPARED_STMT_FIELD = "preparedStmt";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String AWS_REGION_SQL_FIELD = "awsRegionSql";
    
    private static final String VARCHAR_TYPE = "varchar";
    private static final String INT_TYPE = "int";
    private static final String BOOLEAN_TYPE = "boolean";
    private static final String TINYINT_TYPE = "tinyint";
    private static final String SMALLINT_TYPE = "smallint";
    private static final String BIGINT_TYPE = "bigint";
    private static final String FLOAT_TYPE = "float";
    private static final String DOUBLE_TYPE = "double";
    private static final String NUMERIC_TYPE = "numeric";
    private static final String ID="id";
    private static final String TABLE_SCHEM = "TABLE_SCHEM";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_TABLE1 = "testTable1";
    private static final String S3_TEST_BUCKET = "s3://testBucket";
    private static final String TEMPLATE_QUERY = "templateVerticaExportQuery";
    
    private QueryFactory queryFactory;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private VerticaMetadataHandler verticaMetadataHandler;
    private VerticaExportQueryBuilder verticaExportQueryBuilder;
    private VerticaSchemaUtils verticaSchemaUtils;
    private Connection connection;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private S3Client amazonS3;
    private FederatedIdentity federatedIdentity;
    private BlockAllocatorImpl allocator;
    private DatabaseMetaData databaseMetaData;
    private TableName tableName;
    private Schema schema;
    private Constraints constraints;
    private SchemaBuilder schemaBuilder;
    private BlockWriter blockWriter;
    private QueryStatusChecker queryStatusChecker;
    private VerticaMetadataHandler verticaMetadataHandlerMocked;
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", VERTICA_NAME,
            "vertica://jdbc:vertica:thin:username/password@//127.0.0.1:1521/vrt");


    @Before
    public void setUp() throws Exception
    {

        this.verticaSchemaUtils = Mockito.mock(VerticaSchemaUtils.class);
        this.queryFactory = Mockito.mock(QueryFactory.class);
        this.verticaExportQueryBuilder = Mockito.mock(VerticaExportQueryBuilder.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        this.tableName = Mockito.mock(TableName.class);
        this.schema = Mockito.mock(Schema.class);
        this.constraints = Mockito.mock(Constraints.class);
        this.schemaBuilder = Mockito.mock(SchemaBuilder.class);
        this.blockWriter = Mockito.mock(BlockWriter.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.amazonS3 = Mockito.mock(S3Client.class);

        Mockito.lenient().when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build())))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);

        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.verticaMetadataHandler = new VerticaMetadataHandler(databaseConnectionConfig, this.jdbcConnectionFactory,
                com.google.common.collect.ImmutableMap.of(), amazonS3, verticaSchemaUtils);
        this.allocator = new BlockAllocatorImpl();
        this.databaseMetaData = this.connection.getMetaData();
        verticaMetadataHandlerMocked = Mockito.spy(this.verticaMetadataHandler);
    }


    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doGetTable() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField(ID)
                .addStringField("name")
                .build();
        Mockito.when(verticaSchemaUtils.buildTableSchema(connection, tableName)).thenReturn(tableSchema);

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap());
        GetTableResponse response = verticaMetadataHandler.doGetTable(allocator, request);

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(tableSchema, response.getSchema());
        assertTrue(response.getPartitionColumns().isEmpty());
    }

    @Test
    public void doListTables() throws Exception
    {
        String[] schema = {TABLE_SCHEM, TABLE_NAME};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE1}};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName(TEST_SCHEMA, TEST_TABLE1));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, tableName.getSchemaName(), null, new String[]{"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListTablesResponse listTablesResponse = this.verticaMetadataHandler.doListTables(this.allocator,
                new ListTablesRequest(this.federatedIdentity,
                        "testQueryId",
                        "testCatalog",
                        tableName.getSchemaName(),
                        null, UNLIMITED_PAGE_SIZE_VALUE));

        Assert.assertArrayEquals(expectedTables.toArray(), listTablesResponse.getTables().toArray());

    }

    @Test
    public void doListSchemaNames() throws Exception
    {

        String[] schema = {TABLE_SCHEM};
        Object[][] values = {{"testDB1"}};
        String[] expected = {"testDB1"};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, null, null, TABLE_TYPES)).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListSchemasResponse listSchemasResponse = this.verticaMetadataHandler.doListSchemaNames(this.allocator,
                new ListSchemasRequest(this.federatedIdentity,
                        "testQueryId", "testCatalog"));

        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void enhancePartitionSchema()
    {
        Set<String> partitionCols = new HashSet<>();
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        this.verticaMetadataHandler.enhancePartitionSchema(schemaBuilder, new GetTableLayoutRequest(
                this.federatedIdentity,
                "queryId",
                "testCatalog",
                this.tableName,
                this.constraints,
                this.schema,
                partitionCols
        ));
        Assert.assertEquals(PREPARED_STMT_FIELD, schemaBuilder.getField(PREPARED_STMT_FIELD).getName());

    }

    @Test
    public void getPartitions() throws Exception {

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addField("bit_col", new ArrowType.Bool()) // BIT
                .addField("tinyint_col", new ArrowType.Int(8, true)) // TINYINT
                .addField("smallint_col", new ArrowType.Int(16, true)) // SMALLINT
                .addIntField("int_col") // INT
                .addField("bigint_col", new ArrowType.Int(64, true)) // BIGINT
                .addField("float_col", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)) // FLOAT4
                .addField("double_col", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)) // FLOAT8
                .addField("decimal_col", new ArrowType.Decimal(10, 2, 128)) // DECIMAL
                .addStringField("varchar_col") // VARCHAR
                .addStringField(PREPARED_STMT_FIELD)
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols(); // Use utility method

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("bit_col", SortedRangeSet.copyOf(new ArrowType.Bool(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Bool(), true)), false));
        constraintsMap.put("tinyint_col", SortedRangeSet.copyOf(new ArrowType.Int(8, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(8, true), (byte) 127)), false));
        constraintsMap.put("smallint_col", SortedRangeSet.copyOf(new ArrowType.Int(16, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(16, true), (short) 32767)), false));
        constraintsMap.put("int_col", SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(32, true), 1000)), false));
        constraintsMap.put("bigint_col", SortedRangeSet.copyOf(new ArrowType.Int(64, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(64, true), 1000000L)), false));
        constraintsMap.put("float_col", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE),
                ImmutableList.of(Range.equal(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), 3.14f)), false));
        constraintsMap.put("double_col", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                ImmutableList.of(Range.equal(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), 3.14159)), false));
        constraintsMap.put("decimal_col", SortedRangeSet.copyOf(new ArrowType.Decimal(10, 2, 128),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Decimal(10, 2, 128), new BigDecimal("123.45"))), false));
        constraintsMap.put("varchar_col", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Utf8(), "test")), false));

        String[] schema = {TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, TYPE_NAME};
        Object[][] values = {
                {TEST_SCHEMA, TEST_TABLE1, "bit_col", BOOLEAN_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "tinyint_col", TINYINT_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "smallint_col", SMALLINT_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "int_col", INT_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "bigint_col", BIGINT_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "float_col", FLOAT_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "double_col", DOUBLE_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "decimal_col", NUMERIC_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, "varchar_col", VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, PREPARED_STMT_FIELD, VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, QUERY_ID_FIELD, VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE1, AWS_REGION_SQL_FIELD, VARCHAR_TYPE}
        };
        int[] types = {
                Types.BOOLEAN, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.FLOAT, Types.DOUBLE, Types.DECIMAL,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
        };

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "queryId" + UUID.randomUUID().toString().replace("-", "");
        String s3ExportBucket = "s3://testS3Bucket";
        String expectedExportSql = String.format(
                "EXPORT TO PARQUET(directory = 's3://s3://testS3Bucket/%s', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                        "AS SELECT bit_col,tinyint_col,smallint_col,int_col,bigint_col,float_col,double_col,decimal_col," +
                        "varchar_col,preparedStmt,queryId,awsRegionSql " +
                        "FROM \"schema1\".\"table1\" " +
                        "WHERE (\"bit_col\" = 1 ) AND (\"tinyint_col\" = 127 ) AND (\"smallint_col\" = 32767 ) AND (\"int_col\" = 1000 ) " +
                        "AND (\"bigint_col\" = 1000000 ) AND (\"float_col\" = 3.14 ) AND (\"double_col\" = 3.14159 ) " +
                        "AND (\"decimal_col\" = 123.45 ) AND (\"varchar_col\" = 'test' )",
                queryId);

        Mockito.when(connection.getMetaData().getColumns(null, "schema1", "table1", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(s3ExportBucket);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("schema1", "table1"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(),null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {
            Block partitions = res.getPartitions();

            String actualQueryID = partitions.getFieldReader("queryId").readText().toString();
            String actualExportSql = partitions.getFieldReader(PREPARED_STMT_FIELD).readText().toString();
            Assert.assertTrue("Actual query ID should start with expected query ID: " + queryId,
                    actualQueryID.startsWith(queryId));

            String normalizedActualExportSql = actualExportSql.replace(actualQueryID, queryId);
            Assert.assertEquals(expectedExportSql, normalizedActualExportSql);
            Assert.assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", partitions.getFieldReader("awsRegionSql").readText().toString());

            for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
        }
    }

    @Test
    public void doGetSplits()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField(PREPARED_STMT_FIELD)
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(PREPARED_STMT_FIELD);
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("day"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("month"), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector("year"), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector(PREPARED_STMT_FIELD), i, "test");
            BlockUtils.setValue(partitions.getFieldVector("queryId"), i, "123");
            BlockUtils.setValue(partitions.getFieldVector("awsRegionSql"), i, "us-west-2");

        }

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("testKey").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT,Collections.emptyMap(),null),
                null);
        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits: req[{}]", req);
        doGetSplitsFunctionTest(req);
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket/testWithFolderPath");
        doGetSplitsFunctionTest(req);
    }

    @Test
    public void doGetSplitsQueryPassthrough() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField(ID)
                .addStringField("name")
                .addStringField(PREPARED_STMT_FIELD)
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(PREPARED_STMT_FIELD);
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector(PREPARED_STMT_FIELD), 0, "SELECT id, name FROM testTable");
        BlockUtils.setValue(partitions.getFieldVector("queryId"), 0, "query123");
        BlockUtils.setValue(partitions.getFieldVector("awsRegionSql"), 0, "ALTER SESSION SET AWSRegion='us-west-2'");

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("query123/part1.parquet").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();

        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("s3://testS3Bucket");
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);


        Mockito.when(connection.prepareStatement("EXPORT TO PARQUET(directory = 's3://testS3Bucket/query123') AS SELECT id, name FROM testTable")).thenReturn(Mockito.mock(PreparedStatement.class));
        Mockito.when(connection.prepareStatement("ALTER SESSION SET AWSRegion='us-west-2'")).thenReturn(Mockito.mock(PreparedStatement.class));

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", JdbcQueryPassthrough.SCHEMA_FUNCTION_NAME), partitions, partitionCols, queryConstraints, null);

        GetSplitsResponse rawResponse = verticaMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        assertEquals(1, rawResponse.getSplits().size());
        Split split = rawResponse.getSplits().iterator().next();
        assertEquals("query123", split.getProperty("query_id"));
        assertEquals("s3:", split.getProperty("exportBucket"));
        assertEquals("query123/part1.parquet", split.getProperty("s3ObjectKey"));
    }


    @Test
    public void testBuildQueryPassthroughSql() {
        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );

        String result = verticaMetadataHandlerMocked.buildQueryPassthroughSql(queryConstraints);
        assertEquals(query, result);
    }

    private void doGetSplitsFunctionTest(GetSplitsRequest req) {
        MetadataResponse rawResponse = verticaMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplits());

        for (Split nextSplit : response.getSplits()) {

            assertNotNull(nextSplit.getProperty("query_id"));
            assertNotNull(nextSplit.getProperty("exportBucket"));
            assertNotNull(nextSplit.getProperty("s3ObjectKey"));
        }

        assertTrue(!response.getSplits().isEmpty());
    }

    @Test
    public void doGetQueryPassthroughSchema() throws Exception {
        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );
        ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        Mockito.when(connection.prepareStatement(query)).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);
        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn(ID);
        Mockito.when(resultSetMetaData.getColumnLabel(1)).thenReturn(ID);
        Mockito.when(resultSetMetaData.getColumnTypeName(1)).thenReturn("INTEGER");
        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("name");
        Mockito.when(resultSetMetaData.getColumnLabel(2)).thenReturn("name");
        Mockito.when(resultSetMetaData.getColumnTypeName(2)).thenReturn("VARCHAR");

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, queryConstraints.getQueryPassthroughArguments());
        GetTableResponse response = verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(2, response.getSchema().getFields().size());
        assertEquals(ID, response.getSchema().getFields().get(0).getName());
        assertEquals("name", response.getSchema().getFields().get(1).getName());
    }

    @Test
    public void doGetQueryPassthroughSchemaInvalid() throws Exception {
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, queryConstraints.getQueryPassthroughArguments());

        try {
            verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No Query passed through"));
        }
    }

    @Test
    public void testComplexExpressionsTest() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField(ID)
                .addStringField("name")
                .addIntField("age")
                .addStringField("status")
                .addStringField(PREPARED_STMT_FIELD)
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();
        
        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(ID, SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(
                        Range.equal(allocator, new ArrowType.Int(32, true), 1),
                        Range.equal(allocator, new ArrowType.Int(32, true), 2),
                        Range.equal(allocator, new ArrowType.Int(32, true), 3)
                ), false));
        constraintsMap.put("age", SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(Range.greaterThan(allocator, new ArrowType.Int(32, true), 18)), false));
        constraintsMap.put("name", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(Range.range(allocator, new ArrowType.Utf8(), "test", true, "tesu", false)), false));
        constraintsMap.put("status", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(
                        Range.lessThan(allocator, new ArrowType.Utf8(), "inactive"),
                        Range.greaterThan(allocator, new ArrowType.Utf8(), "inactive")
                ), false));

        setupMockDatabaseMetadata(TEST_SCHEMA, TEST_TABLE,
                new String[]{ID, "name", "age", "status", PREPARED_STMT_FIELD, QUERY_ID_FIELD, AWS_REGION_SQL_FIELD},
                new String[]{INT_TYPE, VARCHAR_TYPE, INT_TYPE, VARCHAR_TYPE, VARCHAR_TYPE, VARCHAR_TYPE, VARCHAR_TYPE});

        String queryId = "complexExpr" + UUID.randomUUID().toString().replace("-", "");

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName(TEST_SCHEMA, TEST_TABLE),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {

            Block partitions = res.getPartitions();
            String actualQueryID = partitions.getFieldReader("queryId").readText().toString();
            String actualExportSql = partitions.getFieldReader(PREPARED_STMT_FIELD).readText().toString();

            assertTrue("Query ID should start with expected prefix", actualQueryID.startsWith(queryId));
            String normalizedActualSql = actualExportSql.replace(actualQueryID, queryId);

            assertTrue("Should contain EXPORT TO PARQUET", normalizedActualSql.contains("EXPORT TO PARQUET"));
            assertTrue("Should contain SELECT clause", normalizedActualSql.contains("SELECT"));
            assertTrue("Should contain FROM clause", normalizedActualSql.contains("FROM"));
            assertTrue("Should contain IN clause", normalizedActualSql.contains("\"id\" IN ("));
            assertTrue("Should contain age comparison", normalizedActualSql.contains("\"age\" > "));
            assertTrue("Should contain name range", normalizedActualSql.contains("\"name\" >= ") && normalizedActualSql.contains("\"name\" < "));
            assertTrue("Should contain status constraint", normalizedActualSql.contains("status"));
            assertTrue("Should contain AND operators", normalizedActualSql.contains(" AND "));
            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void testComplexExpressionWithRangeAndInPredicatesTest() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("product_id")
                .addField("price", new ArrowType.Decimal(10, 2, 128))
                .addField("created_date", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))
                .addStringField("category")
                .addStringField(PREPARED_STMT_FIELD)
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("product_id", SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(
                        Range.equal(allocator, new ArrowType.Int(32, true), 100),
                        Range.equal(allocator, new ArrowType.Int(32, true), 200),
                        Range.equal(allocator, new ArrowType.Int(32, true), 300)
                ), false));

        constraintsMap.put("price", SortedRangeSet.copyOf(new ArrowType.Decimal(10, 2, 128),
                ImmutableList.of(Range.range(allocator, new ArrowType.Decimal(10, 2, 128),
                        new BigDecimal("10.00"), true, new BigDecimal("500.00"), true)), false));

        constraintsMap.put("category", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(
                        Range.equal(allocator, new ArrowType.Utf8(), "electronics"),
                        Range.equal(allocator, new ArrowType.Utf8(), "books")
                ), false));

        String[] schema = {TABLE_SCHEM, TABLE_NAME, "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"shop", "products", "product_id", "int"},
                {"shop", "products", "price", "numeric"},
                {"shop", "products", "created_date", "date"},
                {"shop", "products", "category", "varchar"},
                {"shop", "products", "preparedStmt", "varchar"},
                {"shop", "products", "queryId", "varchar"},
                {"shop", "products", "awsRegionSql", "varchar"}
        };
        int[] types = {Types.INTEGER, Types.DECIMAL, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "rangeInQuery" + UUID.randomUUID().toString().replace("-", "");

        Mockito.when(connection.getMetaData().getColumns(null, "shop", "products", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(S3_TEST_BUCKET);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("shop", "products"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {

            Block partitions = res.getPartitions();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Range and IN Predicates Test - Actual SQL: {}", actualExportSql);

            assertTrue("Should contain product_id IN clause", actualExportSql.contains("\"product_id\" IN ("));
            assertTrue("Should contain price range", actualExportSql.contains("\"price\" >= ") && actualExportSql.contains("\"price\" <= "));
            assertTrue("Should contain category IN clause", actualExportSql.contains("\"category\" IN ("));
            assertTrue("Should contain multiple AND operators", actualExportSql.contains(" AND "));

            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void testComplexExpressionWithNullableComparisonsTest() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addStringField("nullable_name")
                .addIntField("nullable_age")
                .addStringField("non_nullable_id")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("nullable_name", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(Range.all(allocator, new ArrowType.Utf8())), false));

        constraintsMap.put("nullable_age", SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(), true)); // null allowed with no ranges = IS NULL

        constraintsMap.put("non_nullable_id", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Utf8(), "test123")), false));

        setupMockDatabaseMetadata("users", "profiles",
                new String[]{"nullable_name", "nullable_age", "non_nullable_id", "preparedStmt", "queryId", "awsRegionSql"},
                new String[]{"varchar", "int", "varchar", "varchar", "varchar", "varchar"});

        String queryId = "nullableQuery" + UUID.randomUUID().toString().replace("-", "");

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("users", "profiles"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {

            Block partitions = res.getPartitions();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Nullable Comparisons Test - Actual SQL: {}", actualExportSql);

            assertTrue("Should contain IS NOT NULL", actualExportSql.contains("IS NOT NULL"));
            assertTrue("Should contain IS NULL", actualExportSql.contains("IS NULL"));
            assertTrue("Should contain equality comparison", actualExportSql.contains("\"non_nullable_id\" = "));
            assertTrue("Should contain AND operators", actualExportSql.contains(" AND "));

            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void testComplexExpressionWithDifferentDataTypes() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addField("bool_field", new ArrowType.Bool())
                .addField("tiny_int_field", new ArrowType.Int(8, true))
                .addField("small_int_field", new ArrowType.Int(16, true))
                .addField("big_int_field", new ArrowType.Int(64, true))
                .addField("float_field", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE))
                .addField("double_field", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))
                .addField("decimal_field", new ArrowType.Decimal(15, 3, 128))
                .addField("date_field", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))
                .addStringField("varchar_field")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("bool_field", SortedRangeSet.copyOf(new ArrowType.Bool(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Bool(), true)), false));

        constraintsMap.put("tiny_int_field", SortedRangeSet.copyOf(new ArrowType.Int(8, true),
                ImmutableList.of(Range.range(allocator, new ArrowType.Int(8, true), (byte) 1, true, (byte) 10, true)), false));

        constraintsMap.put("small_int_field", SortedRangeSet.copyOf(new ArrowType.Int(16, true),
                ImmutableList.of(Range.greaterThan(allocator, new ArrowType.Int(16, true), (short) 100)), false));

        constraintsMap.put("big_int_field", SortedRangeSet.copyOf(new ArrowType.Int(64, true),
                ImmutableList.of(Range.lessThanOrEqual(allocator, new ArrowType.Int(64, true), 1000000L)), false));

        constraintsMap.put("float_field", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE),
                ImmutableList.of(Range.equal(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), 3.14f)), false));

        constraintsMap.put("double_field", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                ImmutableList.of(Range.range(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                        1.0, true, 100.0, false)), false));

        constraintsMap.put("decimal_field", SortedRangeSet.copyOf(new ArrowType.Decimal(15, 3, 128),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Decimal(15, 3, 128), new BigDecimal("999.999"))), false));

        constraintsMap.put("varchar_field", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(
                        Range.equal(allocator, new ArrowType.Utf8(), "value1"),
                        Range.equal(allocator, new ArrowType.Utf8(), "value2")
                ), false));

        String[] schema = {TABLE_SCHEM, TABLE_NAME, "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"analytics", "metrics", "bool_field", BOOLEAN_TYPE},
                {"analytics", "metrics", "tiny_int_field", TINYINT_TYPE},
                {"analytics", "metrics", "small_int_field", SMALLINT_TYPE},
                {"analytics", "metrics", "big_int_field", BIGINT_TYPE},
                {"analytics", "metrics", "float_field", FLOAT_TYPE},
                {"analytics", "metrics", "double_field", DOUBLE_TYPE},
                {"analytics", "metrics", "decimal_field", NUMERIC_TYPE},
                {"analytics", "metrics", "date_field", "date"},
                {"analytics", "metrics", "varchar_field", "varchar"},
                {"analytics", "metrics", "preparedStmt", VARCHAR_TYPE},
                {"analytics", "metrics", "queryId", VARCHAR_TYPE},
                {"analytics", "metrics", "awsRegionSql", VARCHAR_TYPE}
        };
        int[] types = {
                Types.BOOLEAN, Types.TINYINT, Types.SMALLINT, Types.BIGINT,
                Types.FLOAT, Types.DOUBLE, Types.DECIMAL, Types.DATE,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
        };

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "dataTypesQuery" + UUID.randomUUID().toString().replace("-", "");

        Mockito.when(connection.getMetaData().getColumns(null, "analytics", "metrics", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(S3_TEST_BUCKET);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("analytics", "metrics"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {

            Block partitions = res.getPartitions();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Different Data Types Test - Actual SQL: {}", actualExportSql);

            // Verify all data types are handled properly
            assertTrue("Should contain boolean comparison", actualExportSql.contains("\"bool_field\" = "));
            assertTrue("Should contain tinyint range", actualExportSql.contains("\"tiny_int_field\" >= ") && actualExportSql.contains("\"tiny_int_field\" <= "));
            assertTrue("Should contain smallint comparison", actualExportSql.contains("\"small_int_field\" > "));
            assertTrue("Should contain bigint comparison", actualExportSql.contains("\"big_int_field\" <= "));
            assertTrue("Should contain float comparison", actualExportSql.contains("\"float_field\" = "));
            assertTrue("Should contain double range", actualExportSql.contains("\"double_field\" >= ") && actualExportSql.contains("\"double_field\" < "));
            assertTrue("Should contain decimal comparison", actualExportSql.contains("\"decimal_field\" = "));
            assertTrue("Should contain varchar IN clause", actualExportSql.contains("\"varchar_field\" IN ("));

            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void testComplexExpressionWithEmptyConstraints() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField(ID)
                .addStringField("name")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>(); // Empty constraints

        String[] schema = {TABLE_SCHEM, TABLE_NAME, "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"test", "empty_table", ID, "int"},
                {"test", "empty_table", "name", "varchar"},
                {"test", "empty_table", "preparedStmt", "varchar"},
                {"test", "empty_table", "queryId", "varchar"},
                {"test", "empty_table", "awsRegionSql", "varchar"}
        };
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "emptyConstraints" + UUID.randomUUID().toString().replace("-", "");
        String expectedExportSql = String.format(
                "EXPORT TO PARQUET(directory = 's3://s3://testBucket/%s', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                        "AS SELECT id,name,preparedStmt,queryId,awsRegionSql FROM \"test\".\"empty_table\"",
                queryId);

        Mockito.when(connection.getMetaData().getColumns(null, "test", "empty_table", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(S3_TEST_BUCKET);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("test", "empty_table"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {

            Block partitions = res.getPartitions();
            String actualQueryID = partitions.getFieldReader("queryId").readText().toString();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Empty Constraints Test - Expected: {}", expectedExportSql);
            logger.info("Empty Constraints Test - Actual: {}", actualExportSql);

            String normalizedActualSql = actualExportSql.replace(actualQueryID, queryId);

            Assert.assertFalse("SQL should not contain WHERE clause", normalizedActualSql.contains("WHERE"));
            assertTrue("SQL should contain EXPORT TO PARQUET", normalizedActualSql.contains("EXPORT TO PARQUET"));
            assertTrue("SQL should contain SELECT clause", normalizedActualSql.contains("SELECT"));
            assertTrue("SQL should contain FROM clause", normalizedActualSql.contains("FROM"));

            Assert.assertEquals("Export SQL should match expected", expectedExportSql, normalizedActualSql);
            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void testComplexExpressionWithInvalidColumnType() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addField("binary_col", new ArrowType.Binary())
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = createStandardPartitionCols();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        byte[] binaryValue = "test_binary_data".getBytes();
        constraintsMap.put("binary_col", SortedRangeSet.copyOf(new ArrowType.Binary(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Binary(), binaryValue)), false));

        String[] schema = {TABLE_SCHEM, TABLE_NAME, "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"test", "binary_table", "binary_col", "varbinary"},
                {"test", "binary_table", "preparedStmt", "varchar"},
                {"test", "binary_table", "queryId", "varchar"},
                {"test", "binary_table", "awsRegionSql", "varchar"}
        };
        int[] types = {Types.VARBINARY, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "invalidType" + UUID.randomUUID().toString().replace("-", "");

        Mockito.when(connection.getMetaData().getColumns(null, "test", "binary_table", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(S3_TEST_BUCKET);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("test", "binary_table"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema, partitionCols)) {

            GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req);
            Block partitions = res.getPartitions();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Invalid Type Test - Actual SQL: {}", actualExportSql);
            assertTrue("Should generate valid SQL", actualExportSql.contains("EXPORT TO PARQUET"));
            assertTrue(partitions.getRowCount() > 0);
            res.close();
        } catch (UnsupportedOperationException e) {
            logger.info("Expected exception for unsupported type: {}", e.getMessage());
            assertTrue("Exception should mention unsupported operation",
                      e.getMessage().toLowerCase().contains("unsupported") ||
                      e.getMessage().toLowerCase().contains("can't handle"));
        }
    }

    private Set<String> createStandardPartitionCols() {
        Set<String> partitionCols = new HashSet<>();
        partitionCols.add(PREPARED_STMT_FIELD);
        partitionCols.add(QUERY_ID_FIELD);
        partitionCols.add(AWS_REGION_SQL_FIELD);
        return partitionCols;
    }

    private void setupMockDatabaseMetadata(String schemaName, String tableName, String[] columnNames, String[] columnTypes) throws Exception {
        String[] schema = {TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, TYPE_NAME};
        Object[][] values = new Object[columnNames.length][4];
        for (int i = 0; i < columnNames.length; i++) {
            values[i] = new Object[]{schemaName, tableName, columnNames[i], columnTypes[i]};
        }

        int[] types = new int[columnNames.length];
        Arrays.fill(types, Types.VARCHAR);

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        Mockito.when(connection.getMetaData().getColumns(null, schemaName, tableName, null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST(TEMPLATE_QUERY)));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(S3_TEST_BUCKET);
    }
}