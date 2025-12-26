/*-
 * #%L
 * athena-sqlserver
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

package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.CredentialsProviderFactory;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerRecordHandler.class);
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    public SqlServerRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SqlServerConstants.NAME, configOptions), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, SqlServerMetadataHandler.JDBC_PROPERTIES,
                new DatabaseConnectionInfo(SqlServerConstants.DRIVER_CLASS, SqlServerConstants.DEFAULT_PORT)), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(),
                AthenaClient.create(), jdbcConnectionFactory, new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final S3Client amazonS3, final SecretsManagerClient secretsManager,
                           final AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints,
                                           Split split) throws SQLException
    {
        PreparedStatement preparedStatement;

        if (constraints.isQueryPassThrough()) {
            preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
        }
        else {
            // Use StringTemplate-based query building
            List<TypeAndValue> parameterValues = new ArrayList<>();
            String sql = SqlServerSqlUtils.buildSql(tableName, schema, constraints, split, parameterValues);
            
            LOGGER.info("Generated SQL: {}", sql);
            preparedStatement = jdbcConnection.prepareStatement(sql);
            
            // Set parameters for the prepared statement
            if (!parameterValues.isEmpty()) {
                setParameters(preparedStatement, parameterValues);
            }
        }
        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);
        return preparedStatement;
    }
    
    private void setParameters(PreparedStatement statement, List<TypeAndValue> parameterValues) throws SQLException
    {
        // Use the same parameter setting logic from JdbcSplitQueryBuilder
        for (int i = 0; i < parameterValues.size(); i++) {
            TypeAndValue typeAndValue = parameterValues.get(i);
            if (typeAndValue == null || typeAndValue.getType() == null) {
                throw new SQLException("TypeAndValue or type is null at index " + i);
            }
            org.apache.arrow.vector.types.Types.MinorType minorTypeForArrowType = 
                org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(typeAndValue.getType());
            
            if (minorTypeForArrowType == null) {
                throw new SQLException("Cannot determine MinorType for ArrowType: " + typeAndValue.getType());
            }

            switch (minorTypeForArrowType) {
                case BIGINT:
                    statement.setLong(i + 1, (long) typeAndValue.getValue());
                    break;
                case INT:
                    statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
                    break;
                case SMALLINT:
                    statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
                    break;
                case TINYINT:
                    statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
                    break;
                case FLOAT8:
                    statement.setDouble(i + 1, (double) typeAndValue.getValue());
                    break;
                case FLOAT4:
                    statement.setFloat(i + 1, (float) typeAndValue.getValue());
                    break;
                case BIT:
                    statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
                    break;
                case DATEDAY:
                    long utcMillis = java.util.concurrent.TimeUnit.DAYS.toMillis(((Number) typeAndValue.getValue()).longValue());
                    java.util.TimeZone aDefault = java.util.TimeZone.getDefault();
                    int offset = aDefault.getOffset(utcMillis);
                    utcMillis -= offset;
                    statement.setDate(i + 1, new java.sql.Date(utcMillis));
                    break;
                case DATEMILLI:
                    java.time.LocalDateTime timestamp = ((java.time.LocalDateTime) typeAndValue.getValue());
                    statement.setTimestamp(i + 1, new java.sql.Timestamp(timestamp.toInstant(java.time.ZoneOffset.UTC).toEpochMilli()));
                    break;
                case VARCHAR:
                    statement.setString(i + 1, String.valueOf(typeAndValue.getValue()));
                    break;
                case VARBINARY:
                    statement.setBytes(i + 1, (byte[]) typeAndValue.getValue());
                    break;
                case DECIMAL:
                    statement.setBigDecimal(i + 1, (java.math.BigDecimal) typeAndValue.getValue());
                    break;
                default:
                    throw new com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException(
                        String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType),
                        software.amazon.awssdk.services.glue.model.ErrorDetails.builder()
                            .errorCode(software.amazon.awssdk.services.glue.model.FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString())
                            .build());
            }
        }
    }

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        return CredentialsProviderFactory.createCredentialProvider(
                getDatabaseConnectionConfig().getSecret(),
                getCachableSecretsManager(),
                new SqlServerOAuthCredentialsProvider()
        );
    }
}
