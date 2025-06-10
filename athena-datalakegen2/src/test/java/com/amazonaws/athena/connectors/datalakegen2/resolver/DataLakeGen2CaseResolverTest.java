/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.resolver.CaseResolver.CASING_MODE_CONFIGURATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DataLakeGen2CaseResolverTest extends TestBase
{
    private Connection mockConnection;
    private PreparedStatement preparedStatement;

    @Before
    public void setUp() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
    }

    @Test
    public void testCaseInsensitiveCaseOnName()
    {
        try {
            String schemaName = "TeStScHeMa";
            String tableName = "TeStTaBlE";
            DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

            String[] schemaCols = {"SCHEMA_NAME"};
            int[] schemaTypes = {Types.VARCHAR};
            Object[][] schemaData = {{schemaName.toLowerCase()}};

            ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));
            when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

            String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(schemaName.toLowerCase(), adjustedSchemaName);

            String[] tableCols = {"TABLE_NAME"};
            int[] tableTypes = {Types.VARCHAR};
            Object[][] tableData = {{tableName.toLowerCase()}};

            ResultSet tableResultSet = mockResultSet(tableCols, tableTypes, tableData, new AtomicInteger(-1));
            when(preparedStatement.executeQuery()).thenReturn(tableResultSet);

            String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(tableName.toLowerCase(), adjustedTableName);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCaseInsensitiveCaseOnObject()
    {
        try {
            String schemaName = "TeStScHeMa";
            String tableName = "TeStTaBlE";
            DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

            ResultSet schemaResultSet = mockResultSet(
                    new String[]{"SCHEMA_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{{schemaName.toLowerCase()}},
                    new AtomicInteger(-1));

            ResultSet tableResultSet = mockResultSet(
                    new String[]{"TABLE_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{{tableName.toLowerCase()}},
                    new AtomicInteger(-1));

            when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);

            TableName adjusted = resolver.getAdjustedTableNameObject(
                    mockConnection,
                    new TableName(schemaName, tableName),
                    Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(new TableName(schemaName.toLowerCase(), tableName.toLowerCase()), adjusted);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCaseInsensitiveSearchNoMatch()
    {
        try {
            String schemaName = "NonExistentSchema";
            DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

            ResultSet emptyResultSet = mockResultSet(
                    new String[]{"SCHEMA_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{},
                    new AtomicInteger(-1));

            when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

            String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
            assertEquals(schemaName, adjustedSchemaName);

            String tableName = "NonExistentTable";
            ResultSet emptyTableResultSet = mockResultSet(
                    new String[]{"TABLE_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{},
                    new AtomicInteger(-1));

            when(preparedStatement.executeQuery()).thenReturn(emptyTableResultSet);

            String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
            assertEquals(tableName, adjustedTableName);
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}