/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters.VertexRowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;

public class VertexRowWriterTest
{

    private RowWriterBuilder mockRowWriterBuilder;
    private Map<String, String> configOptions;

    @BeforeEach
    public void setUp()
    {
        mockRowWriterBuilder = Mockito.mock(RowWriterBuilder.class);
        configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");
    }

    @Test
    public void testWriteRowTemplateWithBit()
    {
        Field field = new Field("bitField", FieldType.nullable(Types.MinorType.BIT.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("bitField", new ArrayList<>(Collections.singletonList("true")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("bitField"), any(BitExtractor.class));
    }

    @Test
    public void testWriteRowTemplateWithVarChar()
    {
        Field field = new Field("varCharField", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("varCharField", new ArrayList<>(Collections.singletonList("testValue")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("varCharField"), any(VarCharExtractor.class));
    }

    @Test
    public void testWriteRowTemplateWithDateMilli()
    {
        Field field = new Field("dateMilliField", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("dateMilliField", new ArrayList<>(Collections.singletonList(new Date())));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("dateMilliField"), any(DateMilliExtractor.class));
    }

    @Test
    public void testWriteRowTemplateWithInt()
    {
        Field field = new Field("intField", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("intField", new ArrayList<>(Collections.singletonList("123")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("intField"), any(IntExtractor.class));
    }

    @Test
    public void testWriteRowTemplateWithBigInt()
    {
        Field field = new Field("bigIntField", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("bigIntField", new ArrayList<>(Collections.singletonList("123456789")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("bigIntField"), any(BigIntExtractor.class));
    }

    @Test
    public void testWriteRowTemplateWithFloat4()
    {
        Field field = new Field("float4Field", FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("float4Field", new ArrayList<>(Collections.singletonList("12.34")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("float4Field"), any(Float4Extractor.class));
    }

    @Test
    public void testWriteRowTemplateWithFloat8()
    {
        Field field = new Field("float8Field", FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        Map<String, Object> context = new HashMap<>();
        context.put("float8Field", new ArrayList<>(Collections.singletonList("12.34")));

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);

        verify(mockRowWriterBuilder).withExtractor(eq("float8Field"), any(Float8Extractor.class));
    }
}
