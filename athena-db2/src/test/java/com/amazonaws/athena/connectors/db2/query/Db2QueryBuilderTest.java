/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.db2.Db2Constants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.db2.Db2MetadataHandler.PARTITIONING_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Db2QueryBuilderTest
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

    private Db2QueryFactory queryFactory;
    private Schema testSchema;
    private Split split;

    @Before
    public void setUp()
    {
        queryFactory = new Db2QueryFactory();
        testSchema = createTestSchema();
        split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.emptyMap());
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

    @Test
    public void getTemplateName_WhenCalled_ReturnsSelectQuery()
    {
        String templateName = Db2QueryBuilder.getTemplateName();
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void build_WithProjectionAndTableName_GeneratesCorrectSelectQuery()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"", sql);
    }

    @Test
    public void getPartitionWhereClauses_WithPartitionInfo_ReturnsDatapartitionnumClause()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        Split partitionSplit = Mockito.mock(Split.class);
        Mockito.when(partitionSplit.getProperty(PARTITION_NUMBER)).thenReturn("0");
        Mockito.when(partitionSplit.getProperty(PARTITIONING_COLUMN)).thenReturn("PC");

        List<String> clauses = builder.getPartitionWhereClauses(partitionSplit);

        assertEquals(List.of(" DATAPARTITIONNUM(PC) = 0"), clauses);
    }

    @Test
    public void getPartitionWhereClauses_WithoutPartitionInfo_ReturnsEmpty()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        Split noPartitionSplit = Mockito.mock(Split.class);
        Mockito.when(noPartitionSplit.getProperty(PARTITIONING_COLUMN)).thenReturn(null);

        List<String> clauses = builder.getPartitionWhereClauses(noPartitionSplit);

        assertEquals(Collections.emptyList(), clauses);
    }

    @Test
    public void build_WithOrderByClause_GeneratesQueryWithOrderBy()
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(COLUMN_SCORE, OrderByField.Direction.DESC_NULLS_LAST));
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split).withOrderByClause(constraints);

        String sql = builder.build();

        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\" ORDER BY \"name\" ASC NULLS FIRST, \"score\" DESC NULLS LAST", sql);
    }

    @Test
    public void build_WithLimitClause_IncludesLimitInSql()
    {
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 100, Collections.emptyMap(), null);
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split).withLimitClause(constraints);

        String sql = builder.build();

        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\" LIMIT 100", sql);
    }

    @Test
    public void build_WithEmptyProjection_GeneratesQueryWithNull()
    {
        Schema emptySchema = new Schema(Collections.emptyList());
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(emptySchema, split);

        String sql = builder.build();

        assertEquals("SELECT null FROM \"test_schema\".\"test_table\"", sql);
    }

    @Test
    public void getSchemaName_WhenCalled_ReturnsQuotedSchemaName()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split);

        assertEquals("Schema name should match", "\"test_schema\"", builder.getSchemaName());
        assertEquals("Table name should match", "\"test_table\"", builder.getTableName());
        assertEquals("Projection should have correct number of columns", 4, builder.getProjection().size());
        assertTrue("Projection should contain id", builder.getProjection().contains("\"id\""));
        assertTrue("Projection should contain name", builder.getProjection().contains("\"name\""));
        assertTrue("Projection should contain active", builder.getProjection().contains("\"active\""));
        assertTrue("Projection should contain score", builder.getProjection().contains("\"score\""));
        assertNotNull("Parameters should not be null", builder.getParameterValues());
    }

    @Test
    public void build_WithAllComponents_GeneratesCompleteQuery()
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(COLUMN_SCORE, OrderByField.Direction.DESC_NULLS_LAST));
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split)
                .withOrderByClause(constraints).withLimitClause(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));

        String sql = builder.build();

        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\" ORDER BY \"name\" ASC NULLS FIRST, \"score\" DESC NULLS LAST", sql);
    }

    @Test
    public void build_WithNullCatalog_DoesNotIncludeCatalogInFromClause()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split);

        String sql = builder.build();

        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"", sql);
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenTableNameNotSet_ThrowsNullPointerException()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenProjectionNotSet_ThrowsNullPointerException()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withTableName(TEST_TABLE);
        builder.build();
    }

    @Test
    public void build_WithRegularColumns_ProjectionUsesQuotedIdentifiers()
    {
        Db2QueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null).withTableName(TEST_TABLE).withProjection(testSchema, split);

        String sql = builder.build();

        assertTrue("SQL projection should use quoted column names", sql.contains("\"id\"") && sql.contains("\"name\""));
    }
}
