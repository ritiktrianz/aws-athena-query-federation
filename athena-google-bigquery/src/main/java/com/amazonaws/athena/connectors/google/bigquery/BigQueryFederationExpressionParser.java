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
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Based on com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser class
 */
public class BigQueryFederationExpressionParser extends FederationExpressionParser
{
    private static final String quoteCharacter = "'";
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();

    public String writeArrayConstructorClause(List<String> arguments)
    {
        ST template = queryFactory.getQueryTemplate("comma_separated_list_spaced");
        template.add("items", arguments);
        return template.render().trim();
    }

    public List<String> parseComplexExpressions(List<Field> columns, Constraints constraints)
    {
        if (constraints.getExpression() == null || constraints.getExpression().isEmpty()) {
            return ImmutableList.of();
        }

        List<FederationExpression> federationExpressions = constraints.getExpression();
        List<String> results = new ArrayList<>();
        for (FederationExpression expression : federationExpressions) {
            String result = parseFunctionCallExpression((FunctionCallExpression) expression);
            results.add(result);
        }
        
        return results;
    }
    /**
     * This is a recursive function, as function calls can have arguments which, themselves, are function calls.
     */
    public String parseFunctionCallExpression(FunctionCallExpression functionCallExpression)
    {
        FunctionName functionName = functionCallExpression.getFunctionName();
        List<FederationExpression> functionArguments = functionCallExpression.getArguments();

        List<String> arguments = new ArrayList<>();
        for (FederationExpression argument : functionArguments) {
            String argumentResult;
            // base cases
            if (argument instanceof ConstantExpression) {
                argumentResult = parseConstantExpression((ConstantExpression) argument);
            }
            else if (argument instanceof VariableExpression) {
                argumentResult = parseVariableExpression((VariableExpression) argument);
            }
            // recursive case
            else if (argument instanceof FunctionCallExpression) {
                argumentResult = parseFunctionCallExpression((FunctionCallExpression) argument);
            }
            else {
                throw new RuntimeException("Should not reach this case - a new subclass was introduced and is not handled.");
            }
            
            arguments.add(argumentResult);
        }

        return mapFunctionToDataSourceSyntax(functionName, functionCallExpression.getType(), arguments);
    }

    public String parseConstantExpression(ConstantExpression constantExpression)
    {
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);

        List<String> constants = new ArrayList<>();
        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            String strVal = BlockUtils.fieldToString(fieldReader);
            constants.add(strVal);
        }
        
        if (constantExpression.getType().equals(ArrowType.Utf8.INSTANCE)
                || constantExpression.getType().equals(ArrowType.LargeUtf8.INSTANCE)
                || fieldReader.getMinorType().equals(Types.MinorType.DATEDAY)) {
            constants = constants.stream()
                    .map(val -> quoteCharacter + val + quoteCharacter)
                    .collect(Collectors.toList());
        }

        return createCommaSeparatedList(constants);
    }

    public String parseVariableExpression(VariableExpression variableExpression)
    {
        return variableExpression.getColumnName();
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.isEmpty()) {
            throw new IllegalArgumentException("Arguments cannot be null or empty.");
        }
        
        switch (operatorType) {
            case UNARY:
                if (arguments.size() != 1) {
                    throw new IllegalArgumentException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case BINARY:
                if (arguments.size() != 2) {
                    throw new IllegalArgumentException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case VARARG:
                break;
            default:
                throw new RuntimeException("A new operator type was introduced without adding support for it.");
        }

        String clause = "";
        
        switch (functionEnum) {
            case ADD_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "+", arguments.get(1));
                break;
            case AND_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "AND", arguments.get(1));
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME:
                clause = writeArrayConstructorClause(arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "/", arguments.get(1));
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "=", arguments.get(1));
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), ">", arguments.get(1));
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), ">=", arguments.get(1));
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                clause = createInExpression(arguments.get(0), arguments.get(1));
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                clause = createIsDistinctFrom(arguments.get(0), arguments.get(1));
                break;
            case IS_NULL_FUNCTION_NAME:
                clause = createIsNullExpression(arguments.get(0));
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "<", arguments.get(1));
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "<=", arguments.get(1));
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                clause = createLikeExpression(arguments.get(0), arguments.get(1));
                break;
            case MODULUS_FUNCTION_NAME:
                clause = createFunctionCall2Args("MOD", arguments.get(0), arguments.get(1));
                break;
            case MULTIPLY_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "*", arguments.get(1));
                break;
            case NEGATE_FUNCTION_NAME:
                clause = createUnaryOperator("-", arguments.get(0));
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "<>", arguments.get(1));
                break;
            case NOT_FUNCTION_NAME:
                clause = createUnaryOperator("NOT", arguments.get(0));
                break;
            case NULLIF_FUNCTION_NAME:
                clause = createFunctionCall2Args("NULLIF", arguments.get(0), arguments.get(1));
                break;
            case OR_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "OR", arguments.get(1));
                break;
            case SUBTRACT_FUNCTION_NAME:
                clause = createBinaryOperator(arguments.get(0), "-", arguments.get(1));
                break;
            default:
                throw new NotImplementedException("The function " + functionName.getFunctionName() + " does not have an implementation");
        }
        
        if (clause == null) {
            return "";
        }
        
        return clause;
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
     * Creates a comma-separated list using StringTemplate
     */
    private static String createCommaSeparatedList(List<String> items)
    {
        ST template = queryFactory.getQueryTemplate("comma_separated_list");
        template.add("items", items);
        return template.render().trim();
    }
}
