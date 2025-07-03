/*-
 * #%L
 * athena-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connectors.google.bigquery.query.BigQueryQueryFactory;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Based on com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser class
 * Now uses StringTemplate for consistency with other BigQuery SQL generation.
 */
public class BigQueryFederationExpressionParser extends FederationExpressionParser
{
    private static final String quoteCharacter = "'";
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryFederationExpressionParser.class);
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();

    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        ST template = queryFactory.getQueryTemplate("comma_separated_list_spaced");
        template.add("items", arguments);
        return template.render().trim();
    }

    public List<String> parseComplexExpressions(List<Field> columns, Constraints constraints)
    {
        LOGGER.info("BigQueryFederationExpressionParser.parseComplexExpressions() - ENTRY");
        LOGGER.info("  Input columns count: {}", columns.size());
        LOGGER.info("  Column names: {}", columns.stream().map(Field::getName).collect(Collectors.toList()));
        
        if (constraints.getExpression() == null || constraints.getExpression().isEmpty()) {
            LOGGER.info("  No federation expressions found in constraints");
            LOGGER.info("BigQueryFederationExpressionParser.parseComplexExpressions() - EXIT (empty)");
            return ImmutableList.of();
        }

        List<FederationExpression> federationExpressions = constraints.getExpression();
        LOGGER.info("  Found {} federation expressions to process", federationExpressions.size());
        
        List<String> results = new ArrayList<>();
        for (int i = 0; i < federationExpressions.size(); i++) {
            FederationExpression expression = federationExpressions.get(i);
            LOGGER.info("  Processing federation expression #{}: {}", i + 1, expression.getClass().getSimpleName());
            
            String result = parseFunctionCallExpression((FunctionCallExpression) expression);
            results.add(result);
            
            LOGGER.info("  Generated SQL clause #{}: {}", i + 1, result);
        }
        
        LOGGER.info("BigQueryFederationExpressionParser.parseComplexExpressions() - EXIT");
        LOGGER.info("  Total generated clauses: {}", results.size());
        LOGGER.info("  Final results: {}", results);
        return results;
    }
    /**
     * This is a recursive function, as function calls can have arguments which, themselves, are function calls.
     * @param functionCallExpression
     * @return
     */
    public String parseFunctionCallExpression(FunctionCallExpression functionCallExpression)
    {
        FunctionName functionName = functionCallExpression.getFunctionName();
        List<FederationExpression> functionArguments = functionCallExpression.getArguments();

        LOGGER.info("  BigQueryFederationExpressionParser.parseFunctionCallExpression() - ENTRY");
        LOGGER.info("    Function name: {}", functionName.getFunctionName());
        LOGGER.info("    Function type: {}", functionCallExpression.getType());
        LOGGER.info("    Arguments count: {}", functionArguments.size());

        List<String> arguments = new ArrayList<>();
        for (int i = 0; i < functionArguments.size(); i++) {
            FederationExpression argument = functionArguments.get(i);
            LOGGER.info("    Processing argument #{}: {}", i + 1, argument.getClass().getSimpleName());
            
            String argumentResult;
            // base cases
            if (argument instanceof ConstantExpression) {
                LOGGER.info("      Parsing ConstantExpression");
                argumentResult = parseConstantExpression((ConstantExpression) argument);
                LOGGER.info("      ConstantExpression result: {}", argumentResult);
            }
            else if (argument instanceof VariableExpression) {
                LOGGER.info("      Parsing VariableExpression");
                argumentResult = parseVariableExpression((VariableExpression) argument);
                LOGGER.info("      VariableExpression result: {}", argumentResult);
            }
            // recursive case
            else if (argument instanceof FunctionCallExpression) {
                LOGGER.info("      Recursively parsing nested FunctionCallExpression");
                argumentResult = parseFunctionCallExpression((FunctionCallExpression) argument);
                LOGGER.info("      Nested FunctionCallExpression result: {}", argumentResult);
            }
            else {
                LOGGER.error("      Unknown expression type: {}", argument.getClass().getSimpleName());
                throw new RuntimeException("Should not reach this case - a new subclass was introduced and is not handled.");
            }
            
            arguments.add(argumentResult);
        }

        LOGGER.info("    All arguments processed: {}", arguments);

        String finalResult = mapFunctionToDataSourceSyntax(functionName, functionCallExpression.getType(), arguments);
        
        LOGGER.info("  BigQueryFederationExpressionParser.parseFunctionCallExpression() - EXIT");
        LOGGER.info("    Final SQL clause: {}", finalResult);
        
        return finalResult;
    }

    public String parseConstantExpression(ConstantExpression constantExpression)
    {
        LOGGER.info("      BigQueryFederationExpressionParser.parseConstantExpression() - ENTRY");
        LOGGER.info("        Constant type: {}", constantExpression.getType());
        
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);

        LOGGER.info("        Block row count: {}", values.getRowCount());

        List<String> constants = new ArrayList<>();
        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            String strVal = BlockUtils.fieldToString(fieldReader);
            constants.add(strVal);
            LOGGER.info("        Constant value #{}: {}", i + 1, strVal);
        }
        
        LOGGER.info("        Raw constants: {}", constants);
        
        if (constantExpression.getType().equals(ArrowType.Utf8.INSTANCE)
                || constantExpression.getType().equals(ArrowType.LargeUtf8.INSTANCE)
                || fieldReader.getMinorType().equals(Types.MinorType.DATEDAY)) {
            LOGGER.info("        Adding quotes to string/date constants");
            constants = constants.stream()
                    .map(val -> quoteCharacter + val + quoteCharacter)
                    .collect(Collectors.toList());
            LOGGER.info("        Quoted constants: {}", constants);
        }

        String result = createCommaSeparatedList(constants);
        LOGGER.info("      BigQueryFederationExpressionParser.parseConstantExpression() - EXIT");
        LOGGER.info("        Final constant result: {}", result);
        return result;
    }

    public String parseVariableExpression(VariableExpression variableExpression)
    {
        LOGGER.info("      BigQueryFederationExpressionParser.parseVariableExpression() - ENTRY");
        LOGGER.info("        Variable name: {}", variableExpression.getColumnName());
        LOGGER.info("        Variable type: {}", variableExpression.getType());
        
        String result = variableExpression.getColumnName();
        
        LOGGER.info("      BigQueryFederationExpressionParser.parseVariableExpression() - EXIT");
        LOGGER.info("        Variable result: {}", result);
        return result;
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        LOGGER.info("    BigQueryFederationExpressionParser.mapFunctionToDataSourceSyntax() - ENTRY");
        LOGGER.info("      Function name: {}", functionName.getFunctionName());
        LOGGER.info("      Return type: {}", type);
        LOGGER.info("      Arguments: {}", arguments);
        
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        LOGGER.info("      Standard function enum: {}", functionEnum);
        LOGGER.info("      Operator type: {}", operatorType);

        if (arguments == null || arguments.size() == 0) {
            LOGGER.error("      Arguments cannot be null or empty");
            throw new IllegalArgumentException("Arguments cannot be null or empty.");
        }
        
        switch (operatorType) {
            case UNARY:
                LOGGER.info("      Validating UNARY function with {} arguments", arguments.size());
                if (arguments.size() != 1) {
                    LOGGER.error("      UNARY function {} requires 1 argument, got {}", functionName.getFunctionName(), arguments.size());
                    throw new IllegalArgumentException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case BINARY:
                LOGGER.info("      Validating BINARY function with {} arguments", arguments.size());
                if (arguments.size() != 2) {
                    LOGGER.error("      BINARY function {} requires 2 arguments, got {}", functionName.getFunctionName(), arguments.size());
                    throw new IllegalArgumentException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case VARARG:
                LOGGER.info("      VARARG function - accepting {} arguments", arguments.size());
                break;
            default:
                LOGGER.error("      Unknown operator type: {}", operatorType);
                throw new RuntimeException("A new operator type was introduced without adding support for it.");
        }

        String clause = "";
        LOGGER.info("      Mapping function {} to BigQuery syntax", functionEnum);
        
        switch (functionEnum) {
            case ADD_FUNCTION_NAME:
                LOGGER.info("      Processing ADD operation: {} + {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "+", arguments.get(1));
                break;
            case AND_FUNCTION_NAME:
                LOGGER.info("      Processing AND operation: {} AND {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "AND", arguments.get(1));
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME:
                LOGGER.info("      Processing ARRAY_CONSTRUCTOR with {} elements", arguments.size());
                clause = writeArrayConstructorClause(type, arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                LOGGER.info("      Processing DIVIDE operation: {} / {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "/", arguments.get(1));
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing EQUAL operation: {} = {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "=", arguments.get(1));
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing GREATER_THAN operation: {} > {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), ">", arguments.get(1));
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing GREATER_THAN_OR_EQUAL operation: {} >= {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), ">=", arguments.get(1));
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                LOGGER.info("      Processing IN operation: {} IN {}", arguments.get(0), arguments.get(1));
                clause = createInExpression(arguments.get(0), arguments.get(1));
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing IS_DISTINCT_FROM operation: {} IS DISTINCT FROM {}", arguments.get(0), arguments.get(1));
                clause = createIsDistinctFrom(arguments.get(0), arguments.get(1));
                break;
            case IS_NULL_FUNCTION_NAME:
                LOGGER.info("      Processing IS_NULL operation: {} IS NULL", arguments.get(0));
                clause = createIsNullExpression(arguments.get(0));
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing LESS_THAN operation: {} < {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "<", arguments.get(1));
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing LESS_THAN_OR_EQUAL operation: {} <= {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "<=", arguments.get(1));
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                LOGGER.info("      Processing LIKE operation: {} LIKE {}", arguments.get(0), arguments.get(1));
                clause = createLikeExpression(arguments.get(0), arguments.get(1));
                break;
            case MODULUS_FUNCTION_NAME:
                LOGGER.info("      Processing MODULUS operation: MOD({}, {})", arguments.get(0), arguments.get(1));
                clause = createFunctionCall2Args("MOD", arguments.get(0), arguments.get(1));
                break;
            case MULTIPLY_FUNCTION_NAME:
                LOGGER.info("      Processing MULTIPLY operation: {} * {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "*", arguments.get(1));
                break;
            case NEGATE_FUNCTION_NAME:
                LOGGER.info("      Processing NEGATE operation: -{}", arguments.get(0));
                clause = createUnaryOperator("-", arguments.get(0));
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                LOGGER.info("      Processing NOT_EQUAL operation: {} <> {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "<>", arguments.get(1));
                break;
            case NOT_FUNCTION_NAME:
                LOGGER.info("      Processing NOT operation: NOT {}", arguments.get(0));
                clause = createUnaryOperator("NOT", arguments.get(0));
                break;
            case NULLIF_FUNCTION_NAME:
                LOGGER.info("      Processing NULLIF operation: NULLIF({}, {})", arguments.get(0), arguments.get(1));
                clause = createFunctionCall2Args("NULLIF", arguments.get(0), arguments.get(1));
                break;
            case OR_FUNCTION_NAME:
                LOGGER.info("      Processing OR operation: {} OR {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "OR", arguments.get(1));
                break;
            case SUBTRACT_FUNCTION_NAME:
                LOGGER.info("      Processing SUBTRACT operation: {} - {}", arguments.get(0), arguments.get(1));
                clause = createBinaryOperator(arguments.get(0), "-", arguments.get(1));
                break;
            default:
                LOGGER.error("      Function {} is not implemented", functionName.getFunctionName());
                throw new NotImplementedException("The function " + functionName.getFunctionName() + " does not have an implementation");
        }
        
        if (clause == null) {
            LOGGER.warn("      Generated clause is null, returning empty string");
            return "";
        }
        
        LOGGER.info("      Generated clause before parentheses: {}", clause);
        String finalResult = createParenthesizedExpression(clause);
        
        LOGGER.info("    BigQueryFederationExpressionParser.mapFunctionToDataSourceSyntax() - EXIT");
        LOGGER.info("      Final result: {}", finalResult);
        return finalResult;
    }

    /**
     * Creates a binary operator expression using StringTemplate
     */
    private static String createBinaryOperator(String left, String operator, String right)
    {
        ST template = queryFactory.getQueryTemplate("binary_operator");
        template.add("left", left);
        template.add("operator", operator);
        template.add("right", right);
        return template.render().trim();
    }

    /**
     * Creates a unary operator expression using StringTemplate
     */
    private static String createUnaryOperator(String operator, String operand)
    {
        ST template = queryFactory.getQueryTemplate("unary_operator");
        template.add("operator", operator);
        template.add("operand", operand);
        return template.render().trim();
    }

    /**
     * Creates a function call with 2 arguments using StringTemplate
     */
    private static String createFunctionCall2Args(String functionName, String arg1, String arg2)
    {
        ST template = queryFactory.getQueryTemplate("function_call_2args");
        template.add("functionName", functionName);
        template.add("arg1", arg1);
        template.add("arg2", arg2);
        return template.render().trim();
    }

    /**
     * Creates an IN expression using StringTemplate
     */
    private static String createInExpression(String column, String values)
    {
        ST template = queryFactory.getQueryTemplate("in_expression");
        template.add("column", column);
        template.add("values", values);
        return template.render().trim();
    }

    /**
     * Creates an IS DISTINCT FROM expression using StringTemplate
     */
    private static String createIsDistinctFrom(String left, String right)
    {
        ST template = queryFactory.getQueryTemplate("is_distinct_from");
        template.add("left", left);
        template.add("right", right);
        return template.render().trim();
    }

    /**
     * Creates an IS NULL expression using StringTemplate
     */
    private static String createIsNullExpression(String operand)
    {
        ST template = queryFactory.getQueryTemplate("is_null_expression");
        template.add("operand", operand);
        return template.render().trim();
    }

    /**
     * Creates a LIKE expression using StringTemplate
     */
    private static String createLikeExpression(String column, String pattern)
    {
        ST template = queryFactory.getQueryTemplate("like_expression");
        template.add("column", column);
        template.add("pattern", pattern);
        return template.render().trim();
    }

    /**
     * Creates a parenthesized expression using StringTemplate
     */
    private static String createParenthesizedExpression(String expression)
    {
        ST template = queryFactory.getQueryTemplate("parenthesized_expression");
        template.add("expression", expression);
        return template.render().trim();
    }

    /**
     * Creates a comma-separated list using StringTemplate
     */
    private static String createCommaSeparatedList(List<String> items)
    {
        ST template = queryFactory.getQueryTemplate("comma_separated_list");
        template.add("items", items);
        return template.render().trim();
    }
}
