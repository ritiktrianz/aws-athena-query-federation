/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.saphana.SaphanaConstants;
import com.amazonaws.athena.connectors.saphana.SaphanaSqlUtils;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_QUOTE_CHARACTER;

public class SaphanaQueryBuilder extends JdbcQueryBuilder<SaphanaQueryBuilder>
{
    protected String partitionClause;

    public SaphanaQueryBuilder(ST template)
    {
        super(template, SAPHANA_QUOTE_CHARACTER);
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new SaphanaPredicateBuilder();
    }
    
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        // SAP HANA uses PARTITION clause in FROM, not WHERE
        return Collections.emptyList();
    }
    
    @Override
    public SaphanaQueryBuilder withTableName(com.amazonaws.athena.connector.lambda.domain.TableName tableName)
    {
        super.withTableName(tableName);
        return this;
    }
    
    /**
     * Sets the partition clause for SAP HANA.
     * SAP HANA uses PARTITION (value) in the FROM clause.
     */
    public SaphanaQueryBuilder withPartitionClause(Split split)
    {
        String partitionValue = split.getProperty(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME);
        
        if (StringUtils.isEmpty(partitionValue) || SaphanaConstants.ALL_PARTITIONS.equals(partitionValue)) {
            this.partitionClause = null;
        }
        else {
            this.partitionClause = JdbcSqlUtils.renderTemplate(
                    SaphanaSqlUtils.getQueryFactory(),
                    "partition_clause",
                    Map.of("partitionValue", partitionValue));
        }
        return this;
    }
    
    /**
     * Getter for partition clause (used by StringTemplate).
     */
    public String getPartitionClause()
    {
        return partitionClause;
    }
    
    /**
     * Transform column for projection, handling spatial columns with ST functions.
     * This is used when we have field children (spatial data types).
     */
    public String transformColumnForProjection(Field field)
    {
        if (field.getChildren() == null || field.getChildren().isEmpty()) {
            return quote(field.getName());
        }
        
        // Handle spatial conversion functions like ST_AsWKT()
        String altFieldName = field.getChildren().get(0).getName();
        if (altFieldName != null && altFieldName.matches("ST_([a-zA-Z]+)\\(\\)")) {
            return altFieldName + " " + quote(field.getName());
        }
        return quote(field.getName());
    }
    
    @Override
    public SaphanaQueryBuilder withProjection(Schema schema, Split split)
    {
        this.projection = schema.getFields().stream()
                .map(field -> {
                    // Check if this is a partition column
                    if (split.getProperties().containsKey(field.getName())) {
                        return null;
                    }
                    // Use the field-aware transformation for spatial columns
                    return transformColumnForProjection(field);
                })
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toList());
        return this;
    }
    
    @Override
    public SaphanaQueryBuilder withLimitClause(Constraints constraints)
    {
        if (constraints.getLimit() > 0) {
            this.limitClause = "LIMIT " + constraints.getLimit();
        }
        return this;
    }
}
