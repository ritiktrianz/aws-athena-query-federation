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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles predicate building for BigQuery constraints, following the pattern established
 * by Timestream and Vertica connectors for better separation of concerns.
 * Uses shared utility methods from BigQueryStorageApiUtils to eliminate code duplication.
 */
public class BigQueryPredicateBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryPredicateBuilder.class);
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();
    
    private BigQueryPredicateBuilder() {}
    
    /**
     * Builds conjuncts (WHERE clause conditions) from schema fields and constraints.
     *
     * @param columns Schema fields to process
     * @param constraints Query constraints from Athena
     * @param parameterValues Output list to collect query parameters
     * @return List of WHERE clause predicates
     */
    public static List<String> buildConjuncts(List<Field> columns, Constraints constraints, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("BigQueryPredicateBuilder.buildConjuncts() - ENTRY");
        LOGGER.info("  Building WHERE clause predicates for {} columns", columns.size());
        LOGGER.info("  Column names: {}", columns.stream().map(Field::getName).collect(java.util.stream.Collectors.toList()));
        
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        
        // Process field-based constraints
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.info("  Processing constraint for column: {}, valueSet: {}, type: {}", column.getName(), valueSet.getClass().getSimpleName(), type.getTypeID());
                    builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                }
                else {
                    LOGGER.info("  No constraints found for column: {}", column.getName());
                }
            }
        }
        
        // Add complex expressions (federation expressions)
        LOGGER.info("  Checking for federation expressions...");
        List<String> complexExpressions = new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints);
        if (!complexExpressions.isEmpty()) {
            LOGGER.info("  Adding {} complex expressions to conjuncts", complexExpressions.size());
            LOGGER.info("  Complex expressions: {}", complexExpressions);
        }
        else {
            LOGGER.info("  No complex expressions found");
        }
        builder.addAll(complexExpressions);
        
        List<String> conjuncts = builder.build();
        LOGGER.info("BigQueryPredicateBuilder.buildConjuncts() - EXIT");
        LOGGER.info("  Generated {} conjuncts with {} parameter values for parameterized query", conjuncts.size(), parameterValues.size());
        LOGGER.info("  Final conjuncts: {}", conjuncts);
        return conjuncts;
    }
    
    /**
     * Converts a ValueSet constraint to a SQL predicate string using StringTemplate.
     */
    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("BigQueryPredicateBuilder.toPredicate(): Building parameterized predicate for column: {}, valueSet: {}, type: {}", 
                    columnName, valueSet.getClass().getSimpleName(), type.getTypeID());
        
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            LOGGER.info("Processing SortedRangeSet for parameterized query - column: {}", columnName);
            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();

            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                LOGGER.info("ValueSet is NONE with NULL allowed for column: {}", columnName);
                return createNullPredicate(columnName, true);
            }

            if (valueSet.isNullAllowed()) {
                LOGGER.info("NULL values allowed for column: {}, adding IS NULL predicate", columnName);
                disjuncts.add(createNullPredicate(columnName, true));
            }

            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                LOGGER.info("ValueSet is unbounded non-null for column: {}", columnName);
                return createNullPredicate(columnName, false);
            }

            SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;
            LOGGER.info("Processing {} ranges for parameterized query - column: {}", sortedRangeSet.getRanges().getOrderedRanges().size(), columnName);
            
            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    LOGGER.info("Found single value range for column: {}, value: {}", columnName, range.getLow().getValue());
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    LOGGER.info("Processing multi-value range for parameterized query - column: {}", columnName);
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                LOGGER.info("Adding ABOVE bound (>) for column: {}", columnName);
                                rangeConjuncts.add(createComparisonPredicate(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                LOGGER.info("Adding EXACTLY (>=) bound for column: {}", columnName);
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
                                LOGGER.info("Adding EXACTLY (<=) upper bound for column: {}", columnName);
                                rangeConjuncts.add(createComparisonPredicate(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                LOGGER.info("Adding BELOW (<) upper bound for column: {}", columnName);
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
                LOGGER.info("Creating equality predicate for single value in column: {}", columnName);
                disjuncts.add(createComparisonPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                LOGGER.info("Creating parameterized IN predicate for {} values in column: {}", singleValues.size(), columnName);
                for (Object value : singleValues) {
                    parameterValues.add(BigQueryStorageApiUtils.getValueForWhereClause(columnName, value, type));
                }
                disjuncts.add(createInPredicate(columnName, singleValues.size()));
            }
        }

        String result = createOrPredicate(disjuncts);
        LOGGER.info("Generated final parameterized predicate for column: {}, result: {}", columnName, result);
        return result;
    }

    /**
     * Creates a simple comparison predicate using StringTemplate.
     */
    private static String createComparisonPredicate(String columnName, String operator, Object value, ArrowType type,
                                      List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("BigQueryPredicateBuilder.createComparisonPredicate(): Creating parameterized comparison - column: {}, operator: {}, value: {}", 
                    columnName, operator, value);
        
        parameterValues.add(BigQueryStorageApiUtils.getValueForWhereClause(columnName, value, type));
        
        ST template = queryFactory.getQueryTemplate("comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        
        String result = template.render().trim();
        LOGGER.info("Generated parameterized comparison predicate: {}", result);
        return result;
    }
    
    /**
     * Creates an IN predicate using StringTemplate.
     */
    private static String createInPredicate(String columnName, int parameterCount)
    {
        LOGGER.info("BigQueryPredicateBuilder.createInPredicate(): Creating parameterized IN predicate - column: {}, parameterCount: {}", 
                    columnName, parameterCount);
        
        ST template = queryFactory.getQueryTemplate("in_predicate");
        template.add("columnName", columnName);
        List<Integer> counts = new ArrayList<>();
        for (int i = 0; i < parameterCount; i++) {
            counts.add(i + 1);
        }
        template.add("counts", counts);
        
        String result = template.render().trim();
        LOGGER.info("Generated parameterized IN predicate: {}", result);
        return result;
    }
    
    /**
     * Creates a NULL predicate using StringTemplate.
     * Moved from BigQueryStorageApiUtils as it's only used by BigQueryPredicateBuilder.
     */
    private static String createNullPredicate(String columnName, boolean isNull)
    {
        LOGGER.info("BigQueryPredicateBuilder.createNullPredicate(): Creating {} predicate for column: {}", 
                    isNull ? "IS NULL" : "IS NOT NULL", columnName);
        
        ST template = queryFactory.getQueryTemplate("null_predicate");
        template.add("columnName", columnName);
        template.add("isNull", isNull);
        
        String result = template.render().trim();
        LOGGER.info("Generated NULL predicate: {}", result);
        return result;
    }
    
    /**
     * Creates a range predicate using StringTemplate.
     * Moved from BigQueryStorageApiUtils as it's only used by BigQueryPredicateBuilder.
     */
    private static String createRangePredicate(List<String> conjuncts)
    {
        LOGGER.info("BigQueryPredicateBuilder.createRangePredicate(): Creating range predicate with {} conjuncts", conjuncts.size());
        
        ST template = queryFactory.getQueryTemplate("range_predicate");
        template.add("conjuncts", conjuncts);
        
        String result = template.render().trim();
        LOGGER.info("Generated range predicate: {}", result);
        return result;
    }
    
    /**
     * Creates an OR predicate using StringTemplate.
     * Moved from BigQueryStorageApiUtils as it's only used by BigQueryPredicateBuilder.
     */
    private static String createOrPredicate(List<String> disjuncts)
    {
        LOGGER.info("BigQueryPredicateBuilder.createOrPredicate(): Creating OR predicate with {} disjuncts", disjuncts.size());
        
        if (disjuncts.isEmpty()) {
            LOGGER.info("No disjuncts provided, returning empty string");
            return "";
        }
        if (disjuncts.size() == 1) {
            String singleDisjunct = disjuncts.get(0);
            if (singleDisjunct.contains(" AND ")) {
                LOGGER.info("Single range predicate, adding extra parentheses: {}", singleDisjunct);
                return "(" + singleDisjunct + ")";
            }
            else {
                LOGGER.info("Single simple predicate, returning as-is: {}", singleDisjunct);
                return singleDisjunct;
            }
        }
        
        ST template = queryFactory.getQueryTemplate("or_predicate");
        template.add("disjuncts", disjuncts);
        
        String result = template.render().trim();
        LOGGER.info("Generated OR predicate: {}", result);
        return result;
    }
}
