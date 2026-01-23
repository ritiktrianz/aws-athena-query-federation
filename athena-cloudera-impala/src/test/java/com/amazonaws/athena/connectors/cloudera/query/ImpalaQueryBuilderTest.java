/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connectors.cloudera.ImpalaConstants;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
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
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ImpalaQueryBuilderTest
{
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";

    @Mock
    private Split split;

    private ImpalaQueryBuilder queryBuilder;

    @Before
    public void setUp()
    {
        ImpalaQueryFactory queryFactory = new ImpalaQueryFactory();
        queryBuilder = queryFactory.createQueryBuilder();
        createSplitWithPartition("p0");
    }

    @Test
    public void build_WithBasicQuery_GeneratesSelectFromQuery()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("col2", Types.MinorType.VARCHAR.getType()).build())
                .build();
        TableName tableName = createTableName();
        Constraints constraints = createConstraints(Constraints.DEFAULT_NO_LIMIT, Collections.emptyList());

        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .build();

        String expectedSql = "SELECT col1, col2 FROM testSchema.testTable  WHERE p0";
        assertEquals("SQL should match expected", expectedSql, sql);
    }

    @Test
    public void build_WithOrderByClause_GeneratesQueryWithOrderBy()
    {
        Schema schema = createSchema();
        TableName tableName = createTableName();
        List<OrderByField> orderByFields = Collections.singletonList(new OrderByField("col1", OrderByField.Direction.ASC_NULLS_LAST));
        Constraints constraints = createConstraints(Constraints.DEFAULT_NO_LIMIT, orderByFields);

        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .build();

        String expectedSql = "SELECT col1 FROM testSchema.testTable  WHERE p0 ORDER BY col1 ASC NULLS LAST";
        assertEquals("SQL should match expected", expectedSql, sql);
    }

    @Test
    public void build_WithLimitClause_GeneratesQueryWithLimit()
    {
        Schema schema = createSchema();
        TableName tableName = createTableName();
        Constraints constraints = createConstraints(10L, Collections.emptyList());

        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withLimitClause(constraints)
                .build();

        String expectedSql = "SELECT col1 FROM testSchema.testTable  WHERE p0 LIMIT 10";
        assertEquals("SQL should match expected", expectedSql, sql);
    }

    @Test
    public void build_WithPartition_IncludesPartitionInWhereClause()
    {
        Schema schema = createSchema();
        TableName tableName = createTableName();
        Constraints constraints = createConstraints(Constraints.DEFAULT_NO_LIMIT, Collections.emptyList());

        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .build();

        String expectedSql = "SELECT col1 FROM testSchema.testTable  WHERE p0";
        assertEquals("SQL should match expected", expectedSql, sql);
    }

    @Test
    public void build_WithAllPartitions_DoesNotIncludePartitionInWhereClause()
    {
        createSplitWithPartition("*");
        Schema schema = createSchema();
        TableName tableName = createTableName();
        Constraints constraints = createConstraints(Constraints.DEFAULT_NO_LIMIT, Collections.emptyList());

        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .build();

        // When partition is "*", it should not be included in WHERE clause
        String expectedSql = "SELECT col1 FROM testSchema.testTable";
        assertEquals("SQL should match expected", expectedSql, sql);
    }

    @Test
    public void getPartitionWhereClauses_WithValidPartition_ReturnsPartitionClause()
    {
        List<String> partitionClauses = queryBuilder.getPartitionWhereClauses(split);
        assertEquals("Should have one partition clause", 1, partitionClauses.size());
        assertEquals("Partition clause should match", "p0", partitionClauses.get(0));
    }

    @Test
    public void getPartitionWhereClauses_WithAllPartitions_ReturnsEmptyList()
    {
        createSplitWithPartition("*");
        List<String> partitionClauses = queryBuilder.getPartitionWhereClauses(split);
        assertEquals("Should have no partition clauses", 0, partitionClauses.size());
    }

    private Schema createSchema()
    {
        return SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                .build();
    }

    private TableName createTableName()
    {
        return new TableName(TEST_SCHEMA, TEST_TABLE);
    }

    private Constraints createConstraints(long limit, List<OrderByField> orderByFields)
    {
        return new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                limit,
                Collections.emptyMap(),
                null
        );
    }

    private void createSplitWithPartition(String partitionValue)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME, partitionValue);
        when(split.getProperties()).thenReturn(properties);
        when(split.getProperty(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(partitionValue);
    }
}
