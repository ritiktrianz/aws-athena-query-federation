/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.Enums.SpecialKeys;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VertexRowWriterTest
{
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final String NULL_FIELD = "nullField";
    public static final String EMPTY_FIELD = "emptyField";
    public static final String NAME = "name";
    public static final String MULTI_FIELD = "multiField";
    public static final String EMPTY_LIST = "emptyList";
    public static final String CREATED_DATE = "createdDate";
    public static final String AGE = "age";
    public static final String BIG_NUMBER = "bigNumber";
    public static final String SCORE = "score";
    public static final String PRECISION = "precision";
    public static final String NAME_CAPS = "NAME";
    public static final String TESTFIELD = "TESTFIELD";

    // Test values
    private static final String VERTEX_123 = "vertex123";
    private static final String PERSON_LABEL = "person";
    private static final String JOHN_DOE = "John Doe";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String VALUE_3 = "value3";
    private static final String SEMICOLON_SEPARATED = ";";
    private static final String TEST_VALUE = "Test Value";
    private static final String SPACES = "   ";
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    private static final String TESTFIELD_LOWER = "testfield";

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;

    @Before
    public void setUp()
    {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        configOptions = new HashMap<>();
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), VERTEX_123);
        vertexContext.put(T.label.toString(), PERSON_LABEL);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndValidBoolean_ExtractsCorrectValue() throws Exception
    {
        addToVertexContext(ACTIVE, true);
        BitExtractor extractor = captureBitExtractor(boolField(ACTIVE));
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndValidFalse_ExtractsCorrectValue() throws Exception
    {
        addToVertexContext(INACTIVE, false);
        BitExtractor extractor = captureBitExtractor(boolField(INACTIVE));
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndNullValue_HandlesNullValue() throws Exception
    {
        vertexContext.put(NULL_FIELD, null);
        BitExtractor extractor = captureBitExtractor(boolField(NULL_FIELD));
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_HandlesEmptyString() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        vertexContext.put(EMPTY_FIELD, emptyValues);
        BitExtractor extractor = captureBitExtractor(boolField(EMPTY_FIELD));
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndRegularField_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> nameValues = new ArrayList<>();
        nameValues.add(JOHN_DOE);
        vertexContext.put(NAME, nameValues);
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(NAME));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSpecialIDField_HandlesSpecialIDField() throws Exception
    {
        Field idField = utf8Field(SpecialKeys.ID.toString().toLowerCase());
        VarCharExtractor extractor = captureVarCharExtractor(idField);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndMultipleValues_ConcatenatesValues() throws Exception
    {
        ArrayList<Object> multiValues = new ArrayList<>();
        multiValues.add(VALUE_1);
        multiValues.add(VALUE_2);
        multiValues.add(VALUE_3);
        vertexContext.put(MULTI_FIELD, multiValues);
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(MULTI_FIELD));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VALUE_1 + SEMICOLON_SEPARATED + VALUE_2 + SEMICOLON_SEPARATED + VALUE_3, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndEmptyArrayList_HandlesEmptyArrayList() throws Exception
    {
        vertexContext.put(EMPTY_LIST, new ArrayList<>());
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(EMPTY_LIST));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndValidDate_ExtractsCorrectValue() throws Exception
    {
        Date testDate = new Date();
        ArrayList<Object> dateValues = new ArrayList<>();
        dateValues.add(testDate);
        vertexContext.put(CREATED_DATE, dateValues);
        DateMilliExtractor extractor = captureDateMilliExtractor(dateMilliField(CREATED_DATE));
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndValidInteger_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> intValues = new ArrayList<>();
        intValues.add(30);
        vertexContext.put(AGE, intValues);
        IntExtractor extractor = captureIntExtractor(int32Field(AGE));
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(30, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndValidLong_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> longValues = new ArrayList<>();
        longValues.add(9223372036854775807L);
        vertexContext.put(BIG_NUMBER, longValues);
        BigIntExtractor extractor = captureBigIntExtractor(int64Field(BIG_NUMBER));
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(9223372036854775807L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndValidFloat_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> floatValues = new ArrayList<>();
        floatValues.add(3.14f);
        vertexContext.put(SCORE, floatValues);
        Float4Extractor extractor = captureFloat4Extractor(float4Field(SCORE));
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(3.14f, holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndValidDouble_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> doubleValues = new ArrayList<>();
        doubleValues.add(2.718281828);
        vertexContext.put(PRECISION, doubleValues);
        Float8Extractor extractor = captureFloat8Extractor(float8Field(PRECISION));
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.718281828, holder.value, 0.000001);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseInsensitive_HandlesCaseInsensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        addToVertexContext(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(NAME_CAPS));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseSensitive_HandlesCaseSensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        addToVertexContext(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(NAME_CAPS));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndDefaultCaseInsensitive_HandlesDefaultCaseInsensitive() throws Exception
    {
        ArrayList<Object> nameValues = new ArrayList<>();
        nameValues.add(TEST_VALUE);
        vertexContext.put(TESTFIELD_LOWER, nameValues);
        VarCharExtractor extractor = captureVarCharExtractor(utf8Field(TESTFIELD));
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndIDFieldTransformation_HandlesIDFieldTransformation() throws Exception
    {
        Field idField = utf8Field(SpecialKeys.ID.toString().toLowerCase());
        VarCharExtractor extractor = captureVarCharExtractor(idField);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullRowWriterBuilder_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(null, utf8Field(NAME), configOptions);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullField_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, null, configOptions);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullConfigOptions_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, utf8Field(NAME), null);
    }

    private BitExtractor captureBitExtractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<BitExtractor> captor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private VarCharExtractor captureVarCharExtractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<VarCharExtractor> captor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private DateMilliExtractor captureDateMilliExtractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<DateMilliExtractor> captor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private IntExtractor captureIntExtractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<IntExtractor> captor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private BigIntExtractor captureBigIntExtractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<BigIntExtractor> captor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private Float4Extractor captureFloat4Extractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<Float4Extractor> captor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private Float8Extractor captureFloat8Extractor(Field field)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<Float8Extractor> captor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private static Field boolField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
    }

    private static Field utf8Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static Field dateMilliField(String name)
    {
        return new Field(name, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
    }

    private static Field int32Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
    }

    private static Field int64Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
    }

    private static Field float4Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    }

    private static Field float8Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    }

    private void setupSchemaCaseInsensitive(String caseInsensitiveValue)
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, caseInsensitiveValue);
    }

    private void addToVertexContext(String fieldName, Object value)
    {
        if (value instanceof ArrayList) {
            vertexContext.put(fieldName, value);
        }
        else {
            ArrayList<Object> list = new ArrayList<>();
            list.add(value);
            vertexContext.put(fieldName, list);
        }
    }
}
