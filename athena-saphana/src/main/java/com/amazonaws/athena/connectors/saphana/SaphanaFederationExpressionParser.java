/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.TemplateBasedJdbcFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.Map;

/**
 * SAP HANA implementation of FederationExpressionParser using StringTemplate.
 * Extends TemplateBasedJdbcFederationExpressionParser which provides the common
 * template-based implementation for all migrated JDBC connectors.
 */
public class SaphanaFederationExpressionParser extends TemplateBasedJdbcFederationExpressionParser
{
    public SaphanaFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    protected JdbcQueryFactory getQueryFactory()
    {
        return SaphanaSqlUtils.getQueryFactory();
    }

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        // SAP HANA uses comma-separated list without parentheses for array constructor
        return JdbcSqlUtils.renderTemplate(
                getQueryFactory(),
                "comma_separated_list",
                Map.of("items", arguments));
    }
}
