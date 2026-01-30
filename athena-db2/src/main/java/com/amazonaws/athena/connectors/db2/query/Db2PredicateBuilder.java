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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.db2.Db2FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.amazonaws.athena.connectors.db2.Db2Constants.QUOTE_CHARACTER;

public class Db2PredicateBuilder extends JdbcPredicateBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2PredicateBuilder.class);

    public Db2PredicateBuilder()
    {
        super(QUOTE_CHARACTER, new Db2QueryFactory());
    }

    @Override
    public List<String> buildConjuncts(List<Field> columns, Constraints constraints,
                                       List<TypeAndValue> parameterValues, Split split)
    {
        LOGGER.debug("Inside buildConjuncts(): ");
        List<String> builder = super.buildConjuncts(columns, constraints, parameterValues, split);

        // Add complex expressions (federation expressions)
        builder.addAll(new Db2FederationExpressionParser(QUOTE_CHARACTER).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }
}
