/*-
 * #%L
 * athena-cloudera-hive
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
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

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.FETCH_SIZE;
import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_NAME;
import static com.amazonaws.athena.connectors.cloudera.HiveConstants.JDBC_PROPERTIES;

public class HiveRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveRecordHandler.class);
    
    public HiveRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(HIVE_NAME, configOptions), configOptions);
    }
    public HiveRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new HiveJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(HIVE_DRIVER_CLASS, HIVE_DEFAULT_PORT)), configOptions);
    }
    public HiveRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                jdbcConnectionFactory, configOptions);
    }
    @VisibleForTesting
    HiveRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
    }
    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split) throws SQLException
    {
        PreparedStatement preparedStatement;

        if (constraints.isQueryPassThrough()) {
            preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
        }
        else {
            // Use StringTemplate-based query building
            List<TypeAndValue> parameterValues = new ArrayList<>();
            String sql = HiveSqlUtils.buildSql(tableName, schema, constraints, split, parameterValues);
            
            LOGGER.info("Generated SQL: {}", sql);
            preparedStatement = jdbcConnection.prepareStatement(sql);
            
            // Set parameters for the prepared statement
            if (!parameterValues.isEmpty()) {
                JdbcSqlUtils.setParameters(preparedStatement, parameterValues);
            }
        }
        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);
        return preparedStatement;
    }
}
