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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.cloudera.ImpalaFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_QUOTE_CHARACTER;

/**
 * Predicate builder for Impala.
 */
public class ImpalaPredicateBuilder extends JdbcPredicateBuilder
{
    public ImpalaPredicateBuilder()
    {
        super(IMPALA_QUOTE_CHARACTER, new ImpalaQueryFactory());
    }

    @Override
    public List<String> buildConjuncts(List<Field> columns, Constraints constraints,
                                       List<TypeAndValue> parameterValues, Split split)
    {
        List<String> builder = super.buildConjuncts(columns, constraints, parameterValues, split);

        // Add complex expressions (federation expressions)
        builder.addAll(new ImpalaFederationExpressionParser(IMPALA_QUOTE_CHARACTER).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }
}
