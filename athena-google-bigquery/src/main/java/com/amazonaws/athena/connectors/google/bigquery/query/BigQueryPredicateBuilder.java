/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery.query;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.google.bigquery.BigQueryFederationExpressionParser;
import com.amazonaws.athena.connectors.google.bigquery.BigQueryStorageApiUtils;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;

public class BigQueryPredicateBuilder
{
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();
    
    private BigQueryPredicateBuilder() {}

    public static List<String> buildConjuncts(List<Field> columns, Constraints constraints, List<QueryParameterValue> parameterValues)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        
        // Process field-based constraints
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                }
            }
        }
        
        // Add complex expressions (federation expressions)
        List<String> complexExpressions = new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints);
        builder.addAll(complexExpressions);
        
        return builder.build();
    }
    
    /**
     * Converts a ValueSet constraint to a SQL predicate string using StringTemplate.
     */
    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<QueryParameterValue> parameterValues)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();

            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return createNullPredicate(columnName, true);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(createNullPredicate(columnName, true));
            }

            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return createNullPredicate(columnName, false);
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
                                rangeConjuncts.add(createComparisonPredicate(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(createComparisonPredicate(columnName, ">=", range.getLow().getValue(), type, parameterValues));
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
                                rangeConjuncts.add(createComparisonPredicate(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                rangeConjuncts.add(createComparisonPredicate(columnName, "<", range.getHigh().getValue(), type, parameterValues));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add(createRangePredicate(rangeConjuncts));
                }
            }

            if (singleValues.size() == 1) {
                disjuncts.add(createComparisonPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(BigQueryStorageApiUtils.getValueForWhereClause(columnName, value, type));
                }
                disjuncts.add(createInPredicate(columnName, singleValues.size()));
            }
        }

        return createOrPredicate(disjuncts);
    }

    /**
     * Creates a simple comparison predicate using StringTemplate.
     */
    private static String createComparisonPredicate(String columnName, String operator, Object value, ArrowType type,
                                      List<QueryParameterValue> parameterValues)
    {
        parameterValues.add(BigQueryStorageApiUtils.getValueForWhereClause(columnName, value, type));
        
        ST template = queryFactory.getQueryTemplate("comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        
        return template.render().trim();
    }
    
    /**
     * Creates an IN predicate using StringTemplate.
     */
    private static String createInPredicate(String columnName, int parameterCount)
    {
        ST template = queryFactory.getQueryTemplate("in_predicate");
        template.add("columnName", columnName);
        List<Integer> counts = new ArrayList<>();
        for (int i = 0; i < parameterCount; i++) {
            counts.add(i + 1);
        }
        template.add("counts", counts);
        
        return template.render().trim();
    }
    
    /**
     * Creates a NULL predicate using StringTemplate.
     */
    private static String createNullPredicate(String columnName, boolean isNull)
    {
        ST template = queryFactory.getQueryTemplate("null_predicate");
        template.add("columnName", columnName);
        template.add("isNull", isNull);
        
        return template.render().trim();
    }
    
    /**
     * Creates a range predicate using StringTemplate.
     * */
    private static String createRangePredicate(List<String> conjuncts)
    {
        ST template = queryFactory.getQueryTemplate("range_predicate");
        template.add("conjuncts", conjuncts);
        
        return template.render().trim();
    }
    
    /**
     * Creates an OR predicate using StringTemplate.
     */
    private static String createOrPredicate(List<String> disjuncts)
    {
        if (disjuncts.isEmpty()) {
            return "";
        }
        if (disjuncts.size() == 1) {
            String singleDisjunct = disjuncts.get(0);
            if (singleDisjunct.contains(" AND ")) {
                return "(" + singleDisjunct + ")";
            }
            else {
                return singleDisjunct;
            }
        }
        
        ST template = queryFactory.getQueryTemplate("or_predicate");
        template.add("disjuncts", disjuncts);
        
        return template.render().trim();
    }
}
