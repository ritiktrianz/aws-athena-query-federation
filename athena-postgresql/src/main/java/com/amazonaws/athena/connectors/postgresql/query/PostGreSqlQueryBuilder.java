/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

public class PostGreSqlQueryBuilder extends JdbcQueryBuilder<PostGreSqlQueryBuilder>
{
    
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PostGreSqlQueryBuilder.class);
    private final Connection connection;
    private final String schema;
    private final String table;
    private List<String> charColumns;
    
    public PostGreSqlQueryBuilder(ST template, Connection connection, String schema, String table)
    {
        super(template, POSTGRES_QUOTE_CHARACTER);
        this.connection = connection;
        this.schema = schema;
        this.table = table;
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new PostGreSqlPredicateBuilder();
    }
    
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        
        LOGGER.info("inside postquerybuilder");
        String partitionSchemaName = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
        String partitionName = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);
        
        if ("*".equals(partitionSchemaName) && !"*".equals(partitionName)) {
            return Collections.singletonList(partitionName);
        }
        
        return Collections.emptyList();
    }
    
    @Override
    protected String transformColumnForProjection(String columnName)
    {
        // Lazy load char columns list
        if (charColumns == null) {
            try {
                charColumns = PostGreSqlMetadataHandler.getCharColumns(connection, schema, table);
            }
            catch (SQLException e) {
                // If we can't get char columns, just use default behavior
                charColumns = Collections.emptyList();
            }
        }
        
        // Apply RTRIM to CHAR columns
        if (charColumns.contains(columnName)) {
            return "RTRIM(" + quote(columnName) + ") AS " + quote(columnName);
        }
        
        return quote(columnName);
    }
    
    public PostGreSqlQueryBuilder withPartitionClause(Split split)
    {
        String partitionSchema = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
        String partitionTable = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);
        
        // Only use partition if both are set and not "*"
        if (!PostGreSqlMetadataHandler.ALL_PARTITIONS.equals(partitionSchema)
                && !PostGreSqlMetadataHandler.ALL_PARTITIONS.equals(partitionTable)) {
            // Directly override the schema and table names to point to the partition
            this.schemaName = partitionSchema;
            this.tableName = partitionTable;
        }
        
        return this;
    }
}
