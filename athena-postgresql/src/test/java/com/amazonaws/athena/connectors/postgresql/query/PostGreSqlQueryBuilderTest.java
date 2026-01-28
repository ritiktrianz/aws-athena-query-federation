/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.postgresql.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostGreSqlQueryBuilderTest
{
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final TableName TEST_TABLE = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);

    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private static final ArrowType BIGINT_TYPE = new ArrowType.Int(64, false);
    private static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_ACTIVE = "active";
    private static final String COLUMN_SCORE = "score";

    private static final String SQL_SHOULD_CONTAIN_SELECT = "SQL should contain SELECT";
    private static final String SQL_SHOULD_CONTAIN_ALL_COLUMNS = "SQL should contain all columns";
    private static final String SQL_SHOULD_CONTAIN_FROM_CLAUSE = "SQL should contain FROM clause";

    private PostGreSqlQueryFactory queryFactory;
    private Schema testSchema;
    private Split split;
    private Connection connection;

    @Before
    public void setUp() throws Exception
    {
        queryFactory = new PostGreSqlQueryFactory();
        testSchema = createTestSchema();
        split = createSplit();
        
        // Mock connection for getCharColumns
        connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet charColumnsResultSet = mock(ResultSet.class);
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(charColumnsResultSet);
        when(charColumnsResultSet.next()).thenReturn(false);
    }

    @Test
    public void getTemplateName_WhenCalled_ReturnsSelectQuery()
    {
        String templateName = PostGreSqlQueryBuilder.getTemplateName();

        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void build_WithProjectionAndTableName_GeneratesCorrectSelectQuery()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"id\""));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"name\""));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"active\""));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"score\""));
        assertTrue(SQL_SHOULD_CONTAIN_FROM_CLAUSE, sql.contains("FROM \"test_schema\".\"test_table\""));
    }

    @Test
    public void build_WithOrderByClause_GeneratesQueryWithOrderBy()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraints(orderByFields);

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain ORDER BY clause", sql.contains("ORDER BY"));
        assertTrue("SQL should contain name ordering", sql.contains("\"name\" ASC NULLS FIRST"));
        assertTrue("SQL should contain score ordering", sql.contains("\"score\" DESC NULLS LAST"));
    }

    @Test
    public void build_WithLimitClause_IncludesLimitInSql()
    {
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 100, Collections.emptyMap(), null);
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withLimitClause(constraints);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain LIMIT clause", sql.contains("LIMIT 100"));
    }

    @Test
    public void build_WithEmptyProjection_GeneratesQueryWithNull()
    {
        Schema emptySchema = new Schema(Collections.emptyList());

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(emptySchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue("SQL should contain null for empty projection", sql.contains("null"));
        assertTrue(SQL_SHOULD_CONTAIN_FROM_CLAUSE, sql.contains("FROM \"test_schema\".\"test_table\""));
    }

    @Test
    public void getSchemaName_WhenCalled_ReturnsQuotedSchemaName()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        assertEquals("Schema name should match", "\"test_schema\"", builder.getSchemaName());
        assertEquals("Table name should match", "\"test_table\"", builder.getTableName());
        assertEquals("Projection should have correct number of columns", 4, builder.getProjection().size());
        assertTrue("Projection should contain id", builder.getProjection().contains("\"id\""));
        assertTrue("Projection should contain name", builder.getProjection().contains("\"name\""));
        assertTrue("Projection should contain active", builder.getProjection().contains("\"active\""));
        assertTrue("Projection should contain score", builder.getProjection().contains("\"score\""));

        List<TypeAndValue> parameters = builder.getParameterValues();
        assertNotNull("Parameters should not be null", parameters);
    }

    @Test
    public void build_WithAllComponents_GeneratesCompleteQuery()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraints(orderByFields);

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withCatalog(null); // PostgreSQL doesn't use catalog in FROM
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);
        builder.withLimitClause(createConstraints(Collections.emptyList()));

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue("SQL should contain schema name", sql.contains("test_schema"));
        assertTrue("SQL should contain table name", sql.contains("test_table"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"id\""));
        assertTrue("SQL should contain ORDER BY", sql.contains("ORDER BY"));
    }

    @Test
    public void build_WithEmptySchema_GeneratesQueryWithTableNameOnly()
    {
        TableName tableWithEmptySchema = new TableName("", TEST_TABLE_NAME);

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, "", TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(tableWithEmptySchema);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain table name", sql.contains("\"test_table\""));
    }

    @Test
    public void getSchemaName_WithQuotesInIdentifier_EscapesQuotes()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        // Test getters return quoted identifiers
        assertEquals("Schema name should be quoted", "\"test_schema\"", builder.getSchemaName());
        assertEquals("Table name should be quoted", "\"test_table\"", builder.getTableName());

        // Test that quotes are escaped in identifiers
        TableName tableWithQuotes = new TableName("schema\"name", "table\"name");
        builder.withTableName(tableWithQuotes);
        String schemaName = builder.getSchemaName();
        assertTrue("Quotes should be escaped", schemaName.contains("\"\""));
    }

    @Test
    public void build_WithPartitionClause_ChangesSchemaAndTableName()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, "partition_schema");
        splitProperties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "partition_table");

        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        when(splitWithPartition.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME)).thenReturn("partition_schema");
        when(splitWithPartition.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn("partition_table");

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);
        builder.withPartitionClause(splitWithPartition);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain partition schema", sql.contains("\"partition_schema\""));
        assertTrue("SQL should contain partition table", sql.contains("\"partition_table\""));
        assertFalse("SQL should not contain original schema", sql.contains("\"test_schema\""));
        assertFalse("SQL should not contain original table", sql.contains("\"test_table\""));
    }

    @Test
    public void build_WithAllPartitions_DoesNotChangeSchemaAndTableName()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, PostGreSqlMetadataHandler.ALL_PARTITIONS);
        splitProperties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, PostGreSqlMetadataHandler.ALL_PARTITIONS);

        Split splitWithAllPartitions = mock(Split.class);
        when(splitWithAllPartitions.getProperties()).thenReturn(splitProperties);
        when(splitWithAllPartitions.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))
                .thenReturn(PostGreSqlMetadataHandler.ALL_PARTITIONS);
        when(splitWithAllPartitions.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(PostGreSqlMetadataHandler.ALL_PARTITIONS);

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, splitWithAllPartitions);
        builder.withTableName(TEST_TABLE);
        builder.withPartitionClause(splitWithAllPartitions);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain original schema", sql.contains("\"test_schema\""));
        assertTrue("SQL should contain original table", sql.contains("\"test_table\""));
    }

    @Test
    public void build_WithCharColumn_GeneratesQueryWithRTrim() throws Exception
    {
        // Create a new connection mock for this test with CHAR column detection
        Connection charConnection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet charColumnsResultSet = mock(ResultSet.class);
        
        when(charConnection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(charColumnsResultSet);
        when(charColumnsResultSet.next()).thenReturn(true, false);
        when(charColumnsResultSet.getString("column_name")).thenReturn("name");

        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(charConnection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain RTRIM for CHAR column", sql.contains("RTRIM(\"name\") AS \"name\""));
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenTableNameNotSet_ThrowsNullPointerException()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withProjection(testSchema, split);
        builder.build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenProjectionNotSet_ThrowsNullPointerException()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withTableName(TEST_TABLE);
        builder.build();
    }

    @Test
    public void build_WithNullCatalog_DoesNotIncludeCatalogInFromClause()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        builder.withCatalog(null);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        // Should not contain catalog in FROM clause
        assertFalse("SQL should not contain catalog", sql.contains("catalog"));
    }

    @Test
    public void transformColumnForProjection_WithRegularColumn_ReturnsQuotedFieldName()
    {
        PostGreSqlQueryBuilder builder = queryFactory.createQueryBuilder(connection, TEST_SCHEMA_NAME, TEST_TABLE_NAME);

        // Create a simple field
        Field simpleField = new Field("simple_column", new FieldType(true, STRING_TYPE, null), null);

        String result = builder.transformColumnForProjection(simpleField.getName());

        assertEquals("Should return only quoted field name", 
                "\"simple_column\"", result);
    }

    private Schema createTestSchema()
    {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COLUMN_ID, new FieldType(true, BIGINT_TYPE, null), null));
        fields.add(new Field(COLUMN_NAME, new FieldType(true, STRING_TYPE, null), null));
        fields.add(new Field(COLUMN_ACTIVE, new FieldType(true, BOOLEAN_TYPE, null), null));
        fields.add(new Field(COLUMN_SCORE, new FieldType(true, BIGINT_TYPE, null), null));
        return new Schema(fields);
    }

    private Split createSplit()
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, "*");
        properties.put(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "*");
        when(split.getProperties()).thenReturn(properties);
        when(split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME)).thenReturn("*");
        when(split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn("*");
        return split;
    }

    private List<OrderByField> createOrderByFields()
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(COLUMN_SCORE, OrderByField.Direction.DESC_NULLS_LAST));
        return orderByFields;
    }

    private Constraints createConstraints(List<OrderByField> orderByFields)
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }
}
