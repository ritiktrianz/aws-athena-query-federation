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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ImpalaPredicateBuilderTest
{
    private static final ArrowType INT_TYPE = Types.MinorType.INT.getType();
    private static final ArrowType VARCHAR_TYPE = Types.MinorType.VARCHAR.getType();

    private BlockAllocator allocator;
    private ImpalaPredicateBuilder predicateBuilder;
    private Map<String, ValueSet> constraintMap;
    private List<Field> fields;
    private List<TypeAndValue> parameterValues;

    @Mock
    private Split split;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        predicateBuilder = new ImpalaPredicateBuilder();
        constraintMap = new LinkedHashMap<>();
        fields = new java.util.ArrayList<>();
        parameterValues = new java.util.ArrayList<>();
        org.mockito.Mockito.when(split.getProperties()).thenReturn(Collections.emptyMap());
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildConjuncts_WithSingleValue_ReturnsEqualityPredicate()
    {
        ValueSet valueSet = createSingleValueSet(INT_TYPE, 42);
        constraintMap.put("intCol", valueSet);
        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("intCol"));
        assertTrue("Conjunct should contain =", conjuncts.get(0).contains("="));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleValues_ReturnsInPredicate()
    {
        SortedRangeSet.Builder builder = SortedRangeSet.newBuilder(INT_TYPE, false);
        for (Object value : new Object[]{1, 2, 3}) {
            builder.add(new Range(Marker.exactly(allocator, INT_TYPE, value), Marker.exactly(allocator, INT_TYPE, value)));
        }
        ValueSet valueSet = builder.build();
        constraintMap.put("intCol", valueSet);
        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertEquals("Should have three parameters", 3, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithRange_ReturnsRangePredicate()
    {
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", valueSet);
        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >=", conjuncts.get(0).contains(">="));
        assertTrue("Conjunct should contain <=", conjuncts.get(0).contains("<="));
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNullValue_ReturnsNullPredicate()
    {
        ValueSet valueSet = SortedRangeSet.newBuilder(VARCHAR_TYPE, true).build();
        constraintMap.put("varcharCol", valueSet);
        fields.add(Field.nullable("varcharCol", VARCHAR_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
        assertEquals("Should have no parameters", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleColumns_ReturnsMultipleConjuncts()
    {
        ValueSet valueSet1 = createSingleValueSet(INT_TYPE, 1);
        ValueSet valueSet2 = createSingleValueSet(VARCHAR_TYPE, "test");
        constraintMap.put("intCol", valueSet1);
        constraintMap.put("varcharCol", valueSet2);
        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("varcharCol", VARCHAR_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have two conjuncts", 2, conjuncts.size());
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNoConstraints_ReturnsEmptyList()
    {
        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null), parameterValues, split);

        assertEquals("Should have no conjuncts", 0, conjuncts.size());
    }

    @Test
    public void buildConjuncts_WithPartitionColumn_SkipsPartitionColumn()
    {
        org.mockito.Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partitionCol", "p0"));
        ValueSet valueSet = createSingleValueSet(INT_TYPE, 42);
        constraintMap.put("intCol", valueSet);
        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("partitionCol", INT_TYPE));

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, createConstraints(), parameterValues, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("intCol"));
        assertFalse("Conjunct should not contain partitionCol", conjuncts.get(0).contains("partitionCol"));
    }

    private ValueSet createSingleValueSet(ArrowType type, Object value)
    {
        return SortedRangeSet.newBuilder(type, false)
                .add(new Range(Marker.exactly(allocator, type, value), Marker.exactly(allocator, type, value)))
                .build();
    }

    private Constraints createConstraints()
    {
        return new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }
}
