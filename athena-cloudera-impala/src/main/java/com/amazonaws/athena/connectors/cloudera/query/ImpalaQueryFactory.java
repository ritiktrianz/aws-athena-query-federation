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

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;

/**
 * Factory for creating Impala query templates.
 * Uses JdbcBase.stg directly since Impala doesn't require any special SQL formatting.
 */
public class ImpalaQueryFactory extends JdbcQueryFactory
{
    private static final String JDBC_BASE_TEMPLATE_FILE = "JdbcBase.stg";

    public ImpalaQueryFactory()
    {
        super(JDBC_BASE_TEMPLATE_FILE);
    }

    public ImpalaQueryBuilder createQueryBuilder()
    {
        return new ImpalaQueryBuilder(getQueryTemplate(ImpalaQueryBuilder.getTemplateName()));
    }
}
