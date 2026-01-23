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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.cloudera.ImpalaConstants;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_QUOTE_CHARACTER;

/**
 * Query builder for Impala using StringTemplate.
 */
public class ImpalaQueryBuilder extends JdbcQueryBuilder<ImpalaQueryBuilder>
{
    public ImpalaQueryBuilder(ST template)
    {
        super(template, IMPALA_QUOTE_CHARACTER);
    }

    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new ImpalaPredicateBuilder();
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String partitionClause = split.getProperty(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME);
        if (partitionClause != null && !partitionClause.equals("*")) {
            return Collections.singletonList(partitionClause);
        }
        return Collections.emptyList();
    }
}
