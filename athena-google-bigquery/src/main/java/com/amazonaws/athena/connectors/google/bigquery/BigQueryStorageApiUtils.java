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
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        
        // Process field-based constraints
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    builder.add(toPredicate(column.getName(), valueSet, type));
                }
            }
        }
        
        // Add complex expressions (federation expressions)
        List<String> complexExpressions = new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints);
        builder.addAll(complexExpressions);
        
        return builder.build();
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();

            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return createStorageApiNullPredicate(columnName, true);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(createStorageApiNullPredicate(columnName, true));
            }

            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return createStorageApiNullPredicate(columnName, false);
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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type));
                                break;
                            case EXACTLY:
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type));
                                break;
                            case BELOW:
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
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type));
            }
            else if (singleValues.size() > 1) {
                List<String> val = new ArrayList<>();
                for (Object value : singleValues) {
                    val.add(getValueForWhereClause(columnName, value, type).getValue().toString());
                }
                disjuncts.add(createInPredicateWithValues(columnName, createStorageApiCommaSeparatedList(val)));
            }
        }

        return createStorageApiOrPredicate(disjuncts);
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type)
    {
        return createComparisonPredicateWithValue(columnName, operator, value, type);
    }

    /**
     * Creates a comparison predicate using StringTemplate with inline values for Storage API
     */
    private static String createComparisonPredicateWithValue(String columnName, String operator, Object value, ArrowType type)
    {
        ST template = queryFactory.getQueryTemplate("storage_api_comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        
        String templateResult = template.render().trim();
        String formattedValue = (type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? 
                               quote(getValueForWhereClause(columnName, value, type).getValue()) :
                getValueForWhereClause(columnName, value, type).getValue();
        
        return templateResult.replace("?", formattedValue);
    }
    
    /**
     * Creates an IN predicate using StringTemplate with inline values for Storage API
     */
    private static String createInPredicateWithValues(String columnName, String values)
    {
        ST template = queryFactory.getQueryTemplate("storage_api_in_predicate");
        template.add("columnName", columnName);
        template.add("placeholderList", List.of("?"));
        
        String templateResult = template.render().trim();
        return templateResult.replace("?", values);
    }

    public static ReadSession.TableReadOptions.Builder setConstraints(ReadSession.TableReadOptions.Builder optionsBuilder, Schema schema, Constraints constraints)
    {
        List<String> clauses = buildConjuncts(schema.getFields(), constraints);

        if (!clauses.isEmpty()) {
            String clause = createWhereClause(clauses);
            optionsBuilder = optionsBuilder.setRowRestriction(clause);
        }
        return optionsBuilder;
    }

    /**
     * Creates a WHERE clause by joining conditions with AND using StringTemplate
     */
    private static String createWhereClause(List<String> clauses)
    {
        ST template = queryFactory.getQueryTemplate("where_clause");
        template.add("clauses", clauses);
        
        return template.render().trim();
    }

    /**
     * Converts a value to the appropriate QueryParameterValue for BigQuery.
     */
    public static QueryParameterValue getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        String val;
        StringBuilder tempVal;
        switch (arrowType.getTypeID()) {
            case Int:
                return QueryParameterValue.int64(((Number) value).longValue());
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return QueryParameterValue.numeric(BigDecimal.valueOf((long) value, decimalType.getScale()));
            case FloatingPoint:
                return QueryParameterValue.float64((double) value);
            case Bool:
                return QueryParameterValue.bool((Boolean) value);
            case Utf8:
                return QueryParameterValue.string(value.toString());
            case Date:
                val = value.toString();
                if (val.contains("-")) {
                    tempVal = new StringBuilder(val);
                    tempVal = tempVal.length() == 19 ? tempVal.append(".0") : tempVal;

                    val = String.format("%-26s", tempVal).replace(' ', '0').replace("T", " ");
                    return QueryParameterValue.dateTime(val);
                }
                else {
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
                throw new UnsupportedOperationException("The Arrow type: " + arrowType.getTypeID().name() + " is currently not supported");
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }

    /**
     * Creates a NULL predicate using Storage API StringTemplate (no backticks).
     */
    private static String createStorageApiNullPredicate(String columnName, boolean isNull)
    {
        ST template = queryFactory.getQueryTemplate("storage_api_null_predicate");
        template.add("columnName", columnName);
        template.add("isNull", isNull);

        return template.render().trim();
    }

    /**
     * Creates a range predicate using Storage API StringTemplate (no extra parentheses).
     */
    private static String createStorageApiRangePredicate(List<String> conjuncts)
    {
        ST template = queryFactory.getQueryTemplate("storage_api_range_predicate");
        template.add("conjuncts", conjuncts);

        return template.render().trim();
    }

    /**
     * Creates an OR predicate using Storage API StringTemplate (no extra parentheses).
     */
    private static String createStorageApiOrPredicate(List<String> disjuncts)
    {
        if (disjuncts.isEmpty()) {
            return "";
        }
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        
        ST template = queryFactory.getQueryTemplate("storage_api_or_predicate");
        template.add("disjuncts", disjuncts);

        return template.render().trim();
    }

    /**
     * Creates a comma-separated list using Storage API StringTemplate (no spaces after commas).
     */
    private static String createStorageApiCommaSeparatedList(List<String> items)
    {
        ST template = queryFactory.getQueryTemplate("storage_api_comma_separated_list");
        template.add("items", items);

        return template.render().trim();
    }
}
