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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.postgresql.PostgreSqlFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

public class PostGreSqlPredicateBuilder extends JdbcPredicateBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlPredicateBuilder.class);
    
    public PostGreSqlPredicateBuilder()
    {
        super(POSTGRES_QUOTE_CHARACTER, new PostGreSqlQueryFactory());
    }
    
    @Override
    public List<String> buildConjuncts(List<Field> columns, Constraints constraints,
                                       List<TypeAndValue> parameterValues, Split split)
    {
        LOGGER.debug("Inside buildConjuncts of template(): ");
        List<String> builder = super.buildConjuncts(columns, constraints, parameterValues, split);
        
        // Add complex expressions (federation expressions)
        builder.addAll(new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }
}
