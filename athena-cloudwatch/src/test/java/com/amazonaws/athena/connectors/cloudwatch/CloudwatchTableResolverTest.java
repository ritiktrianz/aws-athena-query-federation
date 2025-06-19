/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchTableResolverTest {
    
    private static final String TEST_LOG_GROUP = "test-group";
    private static final String EXPECTED_FUNCTION_LATEST = "test-function$LATEST";
    private static final String ACTUAL_LOG_GROUP = "test-group";
    private static final String ACTUAL_LOG_STREAM = "test-stream";
    private static final String NO_SUCH_SCHEMA_MESSAGE = "No such schema";
    private static final String GROUP_PREFIX = "group-";
    private static final String STREAM_PREFIX = "stream-";
    private static final String TOKEN_1 = "token1";
    private static final int PAGINATION_SIZE = 10;
    private static final int TOTAL_ITEMS = 20;
    private static final int THROTTLE_LIMIT = 100;
    
    @Mock
    private CloudWatchLogsClient mockAwsLogs;

    @Mock
    private ThrottlingInvoker mockInvoker;

    private CloudwatchTableResolver resolver;

    @Before
    public void setUp() {
        try {
            Mockito.lenient().when(mockInvoker.invoke(Mockito.any()))
                    .thenAnswer(invocation -> {
                        Callable<Object> callable = invocation.getArgument(0);
                        try {
                            return callable.call();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            Mockito.lenient().when(mockInvoker.invoke(Mockito.any(), Mockito.anyLong()))
                    .thenAnswer(invocation -> {
                        Callable<Object> callable = invocation.getArgument(0);
                        try {
                            return callable.call();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            resolver = new CloudwatchTableResolver(mockInvoker, mockAwsLogs, THROTTLE_LIMIT, THROTTLE_LIMIT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLambdaLogStreamPattern() {
        try {
            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(LogGroup.builder().logGroupName(TEST_LOG_GROUP).build())
                    .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(LogStream.builder().logStreamName(EXPECTED_FUNCTION_LATEST).build())
                    .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            CloudwatchTableName result = resolver.validateTable(new TableName(TEST_LOG_GROUP, "test-function$latest"));
            assertEquals(EXPECTED_FUNCTION_LATEST, result.getLogStreamName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCaseInsensitiveMatching() {
        try {
            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(LogGroup.builder().logGroupName(ACTUAL_LOG_GROUP).build())
                    .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(LogStream.builder().logStreamName(ACTUAL_LOG_STREAM).build())
                    .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            CloudwatchTableName result = resolver.validateTable(new TableName("Test-Group", "Test-Stream"));
            assertEquals(ACTUAL_LOG_GROUP, result.getLogGroupName());
            assertEquals(ACTUAL_LOG_STREAM, result.getLogStreamName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSchemaNotFound() {
        doReturn(DescribeLogGroupsResponse.builder().build())
                .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));
        try {
            resolver.validateSchema("non-existent-schema");
            fail("Expected exception");
        } catch (UncheckedExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
            assertThat(e.getCause().getMessage()).contains(NO_SUCH_SCHEMA_MESSAGE);
        }
    }

    @Test
    public void testTableNotFound() {
        doReturn(DescribeLogGroupsResponse.builder()
                .logGroups(LogGroup.builder().logGroupName(TEST_LOG_GROUP).build())
                .build())
                .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

        doReturn(DescribeLogStreamsResponse.builder().build())
                .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

        try {
            resolver.validateTable(new TableName(TEST_LOG_GROUP, "non-existent-stream"));
            fail("Expected exception");
        } catch (UncheckedExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
            assertThat(e.getCause().getMessage()).contains("No such table");
        }
    }

    @Test
    public void testPagination() {
        try {
            List<LogGroup> logGroups = new ArrayList<>();
            List<LogStream> logStreams = new ArrayList<>();

            for (int i = 0; i < TOTAL_ITEMS; i++) {
                logGroups.add(LogGroup.builder().logGroupName(GROUP_PREFIX + i).build());
                logStreams.add(LogStream.builder().logStreamName(STREAM_PREFIX + i).build());
            }

            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(logGroups.subList(0, PAGINATION_SIZE))
                    .nextToken(TOKEN_1)
                    .build())
                    .doReturn(DescribeLogGroupsResponse.builder()
                            .logGroups(logGroups.subList(PAGINATION_SIZE, TOTAL_ITEMS))
                            .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(logStreams.subList(0, PAGINATION_SIZE))
                    .nextToken(TOKEN_1)
                    .build())
                    .doReturn(DescribeLogStreamsResponse.builder()
                            .logStreams(logStreams.subList(PAGINATION_SIZE, TOTAL_ITEMS))
                            .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            try {
                resolver.validateSchema(TEST_LOG_GROUP);
                fail("Expected exception");
            } catch (UncheckedExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
                assertThat(e.getCause().getMessage()).contains(NO_SUCH_SCHEMA_MESSAGE);
            }

            String result = resolver.validateSchema(GROUP_PREFIX + "15");
            assertEquals(GROUP_PREFIX + "15", result);

            CloudwatchTableName tableResult = resolver.validateTable(new TableName(TEST_LOG_GROUP, STREAM_PREFIX + "15"));
            assertEquals(STREAM_PREFIX + "15", tableResult.getLogStreamName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}