/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.db2.Db2SqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.db2.Db2Constants.QUOTE_CHARACTER;

public class Db2QueryBuilder extends JdbcQueryBuilder<Db2QueryBuilder>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2QueryBuilder.class);

    public Db2QueryBuilder(ST template)
    {
        super(template, QUOTE_CHARACTER);
    }

    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new Db2PredicateBuilder();
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String column = split.getProperty("PARTITIONING_COLUMN");
        String partitionNumber = split.getProperty("partition_number");

        if (column != null && partitionNumber != null) {
            LOGGER.debug("Fetching data using Partition");
            // Example query: SELECT * FROM EMP_TABLE WHERE DATAPARTITIONNUM(EMP_NO) = 0
            // DB2 DATAPARTITIONNUM expects unquoted column name (preserving existing behavior)
            String partitionClause = JdbcSqlUtils.renderTemplate(
                    Db2SqlUtils.getQueryFactory(),
                    "partition_clause",
                    Map.of("partitioningColumn", column, "partitionNumber", partitionNumber));
            return Collections.singletonList(" " + partitionClause.trim());
        }

        LOGGER.debug("Fetching data without Partition");
        return Collections.emptyList();
    }
}
