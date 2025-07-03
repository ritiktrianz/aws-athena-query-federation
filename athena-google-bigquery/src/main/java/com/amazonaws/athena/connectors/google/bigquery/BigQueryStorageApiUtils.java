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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.google.bigquery.query.BigQueryQueryFactory;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Utf8;

/**
 * Utilities for BigQuery Storage API operations.
 * Contains methods for building Storage API specific predicates with inline values,
 * and shared utility methods used by both Storage API and predicate building operations.
 */
public class BigQueryStorageApiUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryStorageApiUtils.class);
    private static final String BIGQUERY_QUOTE_CHAR = "\"";
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();

    private BigQueryStorageApiUtils()
    {
        // Utility class - private constructor to prevent instantiation
    }

    private static String quote(final String identifier)
    {
        return BIGQUERY_QUOTE_CHAR + identifier + BIGQUERY_QUOTE_CHAR;
    }

    /**
     * Builds conjuncts (WHERE clause conditions) from schema fields and constraints for Storage API.
     *
     * @param columns Schema fields to process
     * @param constraints Query constraints from Athena
     * @return List of WHERE clause predicates for Storage API
     */
    public static List<String> buildConjuncts(List<Field> columns, Constraints constraints)
    {
        LOGGER.info("BigQueryStorageApiUtils.buildConjuncts() - ENTRY");
        LOGGER.info("  Building Storage API filter expressions for {} columns", columns.size());
        LOGGER.info("  Column names: {}", columns.stream().map(Field::getName).collect(java.util.stream.Collectors.toList()));
        
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        
        // Process field-based constraints
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.info("  Processing Storage API constraint for column: {}, valueSet: {}, type: {}", column.getName(), valueSet.getClass().getSimpleName(), type.getTypeID());
                    builder.add(toPredicate(column.getName(), valueSet, type));
                }
                else {
                    LOGGER.info("  No Storage API constraints found for column: {}", column.getName());
                }
            }
        }
        
        // Add complex expressions (federation expressions)
        LOGGER.info("  Checking for federation expressions in Storage API...");
        List<String> complexExpressions = new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints);
        if (!complexExpressions.isEmpty()) {
            LOGGER.info("  Adding {} complex expressions to Storage API conjuncts", complexExpressions.size());
            LOGGER.info("  Complex expressions: {}", complexExpressions);
        }
        else {
            LOGGER.info("  No complex expressions found for Storage API");
        }
        builder.addAll(complexExpressions);
        
        List<String> conjuncts = builder.build();
        LOGGER.info("BigQueryStorageApiUtils.buildConjuncts() - EXIT");
        LOGGER.info("  Generated {} Storage API conjuncts", conjuncts.size());
        LOGGER.info("  Final Storage API conjuncts: {}", conjuncts);
        return conjuncts;
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type)
    {
        LOGGER.info("BigQueryStorageApiUtils.toPredicate(): Building predicate for column: {}, valueSet: {}, type: {}", 
                    columnName, valueSet.getClass().getSimpleName(), type.getTypeID());
        
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            LOGGER.info("Processing SortedRangeSet for column: {}", columnName);
            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();

            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                LOGGER.info("ValueSet is NONE with NULL allowed for column: {}", columnName);
                return createStorageApiNullPredicate(columnName, true);
            }

            if (valueSet.isNullAllowed()) {
                LOGGER.info("NULL values allowed for column: {}, adding IS NULL predicate", columnName);
                disjuncts.add(createStorageApiNullPredicate(columnName, true));
            }

            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                LOGGER.info("ValueSet is unbounded non-null for column: {}", columnName);
                return createStorageApiNullPredicate(columnName, false);
            }

            SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;
            LOGGER.info("Processing {} ranges for column: {}", sortedRangeSet.getRanges().getOrderedRanges().size(), columnName);
            
            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    LOGGER.info("Found single value range for column: {}, value: {}", columnName, range.getLow().getValue());
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    LOGGER.info("Processing multi-value range for column: {}", columnName);
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                LOGGER.info("Adding ABOVE bound for column: {}", columnName);
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type));
                                break;
                            case EXACTLY:
                                LOGGER.info("Adding EXACTLY (>=) bound for column: {}", columnName);
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type));
                                break;
                            case BELOW:
                                LOGGER.info("Adding BELOW (<) upper bound for column: {}", columnName);
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add(createStorageApiRangePredicate(rangeConjuncts));
                }
            }

            if (singleValues.size() == 1) {
                LOGGER.info("Creating equality predicate for single value in column: {}", columnName);
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type));
            }
            else if (singleValues.size() > 1) {
                LOGGER.info("Creating IN predicate for {} values in column: {}", singleValues.size(), columnName);
                List<String> val = new ArrayList<>();
                for (Object value : singleValues) {
                    val.add(getValueForWhereClause(columnName, value, type).getValue().toString());
                }
                disjuncts.add(createInPredicateWithValues(columnName, createStorageApiCommaSeparatedList(val)));
            }
        }

        String result = createStorageApiOrPredicate(disjuncts);
        LOGGER.info("Generated final predicate for column: {}, result: {}", columnName, result);
        return result;
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type)
    {
        LOGGER.info("BigQueryStorageApiUtils.toPredicate(): Creating comparison predicate - column: {}, operator: {}, value: {}", 
                    columnName, operator, value);
        return createComparisonPredicateWithValue(columnName, operator, value, type);
    }

    /**
     * Creates a comparison predicate using StringTemplate with inline values for Storage API
     */
    private static String createComparisonPredicateWithValue(String columnName, String operator, Object value, ArrowType type)
    {
        LOGGER.info("Creating comparison predicate with inline value for Storage API - column: {}, operator: {}", columnName, operator);
        
        ST template = queryFactory.getQueryTemplate("storage_api_comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        
        String templateResult = template.render().trim();
        String formattedValue = (type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? 
                               quote(getValueForWhereClause(columnName, value, type).getValue()) : 
                               getValueForWhereClause(columnName, value, type).getValue().toString();
        
        String result = templateResult.replace("?", formattedValue);
        LOGGER.info("Generated comparison predicate: {}", result);
        return result;
    }
    
    /**
     * Creates an IN predicate using StringTemplate with inline values for Storage API
     */
    private static String createInPredicateWithValues(String columnName, String values)
    {
        LOGGER.info("Creating IN predicate with inline values for Storage API - column: {}", columnName);
        
        ST template = queryFactory.getQueryTemplate("storage_api_in_predicate");
        template.add("columnName", columnName);
        template.add("placeholderList", Arrays.asList("?"));
        
        String templateResult = template.render().trim();
        String result = templateResult.replace("?", values);
        LOGGER.info("Generated IN predicate: {}", result);
        return result;
    }

    public static ReadSession.TableReadOptions.Builder setConstraints(ReadSession.TableReadOptions.Builder optionsBuilder, Schema schema, Constraints constraints)
    {
        LOGGER.info("BigQueryStorageApiUtils.setConstraints(): Setting constraints for schema with {} fields", schema.getFields().size());
        
        List<String> clauses = buildConjuncts(schema.getFields(), constraints);

        if (!clauses.isEmpty()) {
            String clause = createWhereClause(clauses);
            LOGGER.info("Setting row restriction: {}", clause);
            optionsBuilder = optionsBuilder.setRowRestriction(clause);
        }
        else {
            LOGGER.info("No constraints to apply, skipping row restriction");
        }
        return optionsBuilder;
    }

    /**
     * Creates a WHERE clause by joining conditions with AND using StringTemplate
     */
    private static String createWhereClause(List<String> clauses)
    {
        LOGGER.info("BigQueryStorageApiUtils.createWhereClause(): Creating WHERE clause with {} conditions", clauses.size());
        
        ST template = queryFactory.getQueryTemplate("where_clause");
        template.add("clauses", clauses);
        
        String result = template.render().trim();
        LOGGER.info("Generated WHERE clause: {}", result);
        return result;
    }
    // ========== SHARED UTILITY METHODS ==========

    /**
     * Converts a value to the appropriate QueryParameterValue for BigQuery.
     * Used by both BigQueryPredicateBuilder and BigQueryStorageApiUtils.
     */
    public static QueryParameterValue getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        LOGGER.info("BigQueryStorageApiUtils.getValueForWhereClause(): Processing column={}, value={}, arrowType={}",
                columnName, value, arrowType.getTypeID());

        String val;
        StringBuilder tempVal;
        switch (arrowType.getTypeID()) {
            case Int:
                LOGGER.info("Processing Integer type for column: {}", columnName);
                return QueryParameterValue.int64(((Number) value).longValue());
            case Decimal:
                LOGGER.info("Processing Decimal type for column: {}", columnName);
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return QueryParameterValue.numeric(BigDecimal.valueOf((long) value, decimalType.getScale()));
            case FloatingPoint:
                LOGGER.info("Processing FloatingPoint type for column: {}", columnName);
                return QueryParameterValue.float64((double) value);
            case Bool:
                LOGGER.info("Processing Boolean type for column: {}", columnName);
                return QueryParameterValue.bool((Boolean) value);
            case Utf8:
                LOGGER.info("Processing String type for column: {}", columnName);
                return QueryParameterValue.string(value.toString());
            case Date:
                LOGGER.info("Processing Date type for column: {}", columnName);
                val = value.toString();
                if (val.contains("-")) {
                    LOGGER.info("Processing timestamp format for column: {}", columnName);
                    tempVal = new StringBuilder(val);
                    tempVal = tempVal.length() == 19 ? tempVal.append(".0") : tempVal;

                    val = String.format("%-26s", tempVal).replace(' ', '0').replace("T", " ");
                    return QueryParameterValue.dateTime(val);
                }
                else {
                    LOGGER.info("Processing date format for column: {}", columnName);
                    long days = Long.parseLong(val);
                    long milliseconds = TimeUnit.DAYS.toMillis(days);
                    return QueryParameterValue.date(new Date(milliseconds).toString());
                }
            case Timestamp:
                return QueryParameterValue.dateTime(value.toString());
            case List:
            case FixedSizeList:
            case LargeList:
            case Struct:
            case Union:
            case NONE:
                LOGGER.info("Unsupported Arrow type encountered for column: {}, type: {}", columnName, arrowType.getTypeID().name());
                throw new UnsupportedOperationException("The Arrow type: " + arrowType.getTypeID().name() + " is currently not supported");
            default:
                LOGGER.info("Unknown Arrow type encountered for column: {}, type: {}", columnName, arrowType.getTypeID().name());
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }

    // ========== STORAGE API SPECIFIC METHODS ==========

    /**
     * Creates a NULL predicate using Storage API StringTemplate (no backticks).
     */
    private static String createStorageApiNullPredicate(String columnName, boolean isNull)
    {
        LOGGER.info("Creating Storage API null predicate for column: {}, isNull: {}", columnName, isNull);

        ST template = queryFactory.getQueryTemplate("storage_api_null_predicate");
        template.add("columnName", columnName);
        template.add("isNull", isNull);

        String result = template.render().trim();
        LOGGER.info("Generated Storage API null predicate: {}", result);
        return result;
    }

    /**
     * Creates a range predicate using Storage API StringTemplate (no extra parentheses).
     */
    private static String createStorageApiRangePredicate(List<String> conjuncts)
    {
        LOGGER.info("Creating Storage API range predicate with {} conjuncts", conjuncts.size());

        ST template = queryFactory.getQueryTemplate("storage_api_range_predicate");
        template.add("conjuncts", conjuncts);

        String result = template.render().trim();
        LOGGER.info("Generated Storage API range predicate: {}", result);
        return result;
    }

    /**
     * Creates an OR predicate using Storage API StringTemplate (no extra parentheses).
     */
    private static String createStorageApiOrPredicate(List<String> disjuncts)
    {
        LOGGER.info("Creating Storage API OR predicate with {} disjuncts", disjuncts.size());

        if (disjuncts.isEmpty()) {
            LOGGER.info("No disjuncts provided, returning empty string");
            return "";
        }
        if (disjuncts.size() == 1) {
            LOGGER.info("Single disjunct provided, returning as-is: {}", disjuncts.get(0));
            return disjuncts.get(0);
        }
        
        ST template = queryFactory.getQueryTemplate("storage_api_or_predicate");
        template.add("disjuncts", disjuncts);

        String result = template.render().trim();
        LOGGER.info("Generated Storage API OR predicate: {}", result);
        return result;
    }

    /**
     * Creates a comma-separated list using Storage API StringTemplate (no spaces after commas).
     */
    private static String createStorageApiCommaSeparatedList(List<String> items)
    {
        LOGGER.info("Creating Storage API comma-separated list with {} items", items.size());

        ST template = queryFactory.getQueryTemplate("storage_api_comma_separated_list");
        template.add("items", items);

        String result = template.render().trim();
        LOGGER.info("Generated Storage API comma-separated list: {}", result);
        return result;
    }
}
