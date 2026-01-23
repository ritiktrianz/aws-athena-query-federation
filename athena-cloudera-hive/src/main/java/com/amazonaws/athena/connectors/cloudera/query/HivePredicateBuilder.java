/*-
 * #%L
 * athena-cloudera-hive
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
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.cloudera.HiveFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_QUOTE_CHARACTER;

public class HivePredicateBuilder extends JdbcPredicateBuilder
{
    public HivePredicateBuilder()
    {
        super(HIVE_QUOTE_CHARACTER, new HiveQueryFactory());
    }

    @Override
    public List<String> buildConjuncts(List<Field> columns, Constraints constraints,
                                       List<TypeAndValue> parameterValues, Split split)
    {
        List<String> builder = super.buildConjuncts(columns, constraints, parameterValues, split);

        // Add complex expressions (federation expressions)
        builder.addAll(new HiveFederationExpressionParser(HIVE_QUOTE_CHARACTER).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }

    @Override
    protected String toPredicate(String columnName, ValueSet valueSet, ArrowType type,
                                 List<TypeAndValue> parameterValues)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        boolean isTimestampType = minorType.equals(Types.MinorType.DATEMILLI);
        
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return JdbcSqlUtils.renderTemplate(queryFactory, "null_predicate", Map.of("columnName", quote(columnName), "isNull", true));
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "null_predicate", Map.of("columnName", quote(columnName), "isNull", true)));
            }
            
            List<Range> rangeList = ((SortedRangeSet) valueSet).getOrderedRanges();
            if (rangeList.size() == 1 && !valueSet.isNullAllowed() && rangeList.get(0).getLow().isLowerUnbounded() && rangeList.get(0).getHigh().isUpperUnbounded()) {
                return JdbcSqlUtils.renderTemplate(
                        queryFactory,
                        "null_predicate",
                        Map.of("columnName", quote(columnName), "isNull", false)
                );
            }
            
            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                parameterValues.add(new TypeAndValue(type, range.getLow().getValue()));
                                if (isTimestampType) {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "date_comparison_predicate", Map.of("columnName", quote(columnName), "operator", ">")));
                                }
                                else {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "comparison_predicate", Map.of("columnName", quote(columnName), "operator", ">")));
                                }
                                break;
                            case EXACTLY:
                                parameterValues.add(new TypeAndValue(type, range.getLow().getValue()));
                                if (isTimestampType) {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "date_comparison_predicate", Map.of("columnName", quote(columnName), "operator", ">=")));
                                }
                                else {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "comparison_predicate", Map.of("columnName", quote(columnName), "operator", ">=")));
                                }
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                parameterValues.add(new TypeAndValue(type, range.getHigh().getValue()));
                                if (isTimestampType) {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "date_comparison_predicate", Map.of("columnName", quote(columnName), "operator", "<=")));
                                }
                                else {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "comparison_predicate", Map.of("columnName", quote(columnName), "operator", "<=")));
                                }
                                break;
                            case BELOW:
                                parameterValues.add(new TypeAndValue(type, range.getHigh().getValue()));
                                if (isTimestampType) {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "date_comparison_predicate", Map.of("columnName", quote(columnName), "operator", "<")));
                                }
                                else {
                                    rangeConjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "comparison_predicate", Map.of("columnName", quote(columnName), "operator", "<")));
                                }
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    if (!rangeConjuncts.isEmpty()) {
                        disjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "range_predicate", Map.of("conjuncts", rangeConjuncts)));
                    }
                }
            }

            // Add back all the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                parameterValues.add(new TypeAndValue(type, Iterables.getOnlyElement(singleValues)));
                if (isTimestampType) {
                    disjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "date_comparison_predicate", Map.of("columnName", quote(columnName), "operator", "=")));
                }
                else {
                    disjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "comparison_predicate", Map.of("columnName", quote(columnName), "operator", "=")));
                }
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(new TypeAndValue(type, value));
                }
                List<String> placeholders = Collections.nCopies(singleValues.size(), "?");
                disjuncts.add(JdbcSqlUtils.renderTemplate(queryFactory, "in_predicate", Map.of("columnName", quote(columnName), "counts", placeholders)));
            }
        }

        return JdbcSqlUtils.renderTemplate(queryFactory, "or_predicate", Map.of("disjuncts", disjuncts));
    }
}
