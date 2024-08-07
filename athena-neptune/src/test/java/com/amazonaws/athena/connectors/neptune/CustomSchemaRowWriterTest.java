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
import com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters.CustomSchemaRowWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CustomSchemaRowWriterTest
{

    private RowWriterBuilder rowWriterBuilder;
    private Field field;
    private Map<String, String> configOptions;

    @BeforeEach
    public void setup()
    {
        rowWriterBuilder = mock(RowWriterBuilder.class);
        field = mock(Field.class);
        configOptions = new HashMap<>();
    }

    @Test
    public void testWriteRowTemplateBit()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.Bool());
        when(field.getType()).thenReturn(new ArrowType.Bool());

        Map<String, Object> context = new HashMap<>();
        context.put("testField", true);

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(BitExtractor.class));
    }

    @Test
    public void testWriteRowTemplateVarChar()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.Utf8());

        Map<String, Object> context = new HashMap<>();
        context.put("testField", "testValue");

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(VarCharExtractor.class));
    }

    @Test
    public void testWriteRowTemplateDateMilli()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.Date(DateUnit.MILLISECOND), null);

        Map<String, Object> context = new HashMap<>();
        context.put("testField", new Date());

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(DateMilliExtractor.class));
    }

    @Test
    public void testWriteRowTemplateInt()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.Int(32, true));

        Map<String, Object> context = new HashMap<>();
        context.put("testField", 123);

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(IntExtractor.class));
    }

    @Test
    public void testWriteRowTemplateBigInt()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.Int(64, true));

        Map<String, Object> context = new HashMap<>();
        context.put("testField", 123L);

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(BigIntExtractor.class));
    }

    @Test
    public void testWriteRowTemplateFloat4()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));

        Map<String, Object> context = new HashMap<>();
        context.put("testField", 1.23f);

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(Float4Extractor.class));
    }

    @Test
    public void testWriteRowTemplateFloat8()
    {
        when(field.getName()).thenReturn("testField");
        when(field.getType()).thenReturn(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

        Map<String, Object> context = new HashMap<>();
        context.put("testField", 1.23);

        CustomSchemaRowWriter.writeRowTemplate(rowWriterBuilder, field, configOptions);

        verify(rowWriterBuilder).withExtractor(eq("testField"), any(Float8Extractor.class));
    }
}
