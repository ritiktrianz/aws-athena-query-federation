/*-
 * #%L
 * athena-cloudwatch
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchTableResolverTest {
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
            resolver = new CloudwatchTableResolver(mockInvoker, mockAwsLogs, 100, 100);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLambdaLogStreamPattern() {
        try {
            String logGroup = "test-group";
            String logStream = "test-function$latest";
            String expectedLogStream = "test-function$LATEST";

            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(LogGroup.builder().logGroupName(logGroup).build())
                    .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(LogStream.builder().logStreamName(expectedLogStream).build())
                    .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            CloudwatchTableName result = resolver.validateTable(new TableName(logGroup, logStream));
            assertEquals(expectedLogStream, result.getLogStreamName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCaseInsensitiveMatching() {
        try {
            String logGroup = "Test-Group";
            String logStream = "Test-Stream";
            String actualLogGroup = "test-group";
            String actualLogStream = "test-stream";

            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(LogGroup.builder().logGroupName(actualLogGroup).build())
                    .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(LogStream.builder().logStreamName(actualLogStream).build())
                    .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            CloudwatchTableName result = resolver.validateTable(new TableName(logGroup, logStream));
            assertEquals(actualLogGroup, result.getLogGroupName());
            assertEquals(actualLogStream, result.getLogStreamName());
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
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No such schema"));
        }
    }

    @Test
    public void testTableNotFound() {
        String logGroup = "test-group";
        String logStream = "non-existent-stream";

        doReturn(DescribeLogGroupsResponse.builder()
                .logGroups(LogGroup.builder().logGroupName(logGroup).build())
                .build())
                .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

        doReturn(DescribeLogStreamsResponse.builder().build())
                .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

        try {
            resolver.validateTable(new TableName(logGroup, logStream));
            fail("Expected exception");
        } catch (UncheckedExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("No such table"));
        }
    }

    @Test
    public void testPagination() {
        try {
            String logGroup = "test-group";
            List<LogGroup> logGroups = new ArrayList<>();
            List<LogStream> logStreams = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                logGroups.add(LogGroup.builder().logGroupName("group-" + i).build());
                logStreams.add(LogStream.builder().logStreamName("stream-" + i).build());
            }

            // Mock paginated log groups
            doReturn(DescribeLogGroupsResponse.builder()
                    .logGroups(logGroups.subList(0, 10))
                    .nextToken("token1")
                    .build())
                    .doReturn(DescribeLogGroupsResponse.builder()
                            .logGroups(logGroups.subList(10, 20))
                            .build())
                    .when(mockAwsLogs).describeLogGroups(any(DescribeLogGroupsRequest.class));

            // Mock paginated log streams
            doReturn(DescribeLogStreamsResponse.builder()
                    .logStreams(logStreams.subList(0, 10))
                    .nextToken("token1")
                    .build())
                    .doReturn(DescribeLogStreamsResponse.builder()
                            .logStreams(logStreams.subList(10, 20))
                            .build())
                    .when(mockAwsLogs).describeLogStreams(any(DescribeLogStreamsRequest.class));

            try {
                resolver.validateSchema(logGroup);
                fail("Expected exception");
            } catch (UncheckedExecutionException e) {
                assertTrue(e.getCause() instanceof IllegalArgumentException);
                assertTrue(e.getCause().getMessage().contains("No such schema"));
            }

            String result = resolver.validateSchema("group-15");
            assertEquals("group-15", result);

            CloudwatchTableName tableResult = resolver.validateTable(new TableName(logGroup, "stream-15"));
            assertEquals("stream-15", tableResult.getLogStreamName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}