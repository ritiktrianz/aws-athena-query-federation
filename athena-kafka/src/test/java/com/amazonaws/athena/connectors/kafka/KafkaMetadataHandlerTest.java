/*-
 * #%L
 * Athena Kafka Connector
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
package com.amazonaws.athena.connectors.kafka;


import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.ListRegistriesRequest;
import software.amazon.awssdk.services.glue.model.ListRegistriesResponse;
import software.amazon.awssdk.services.glue.model.RegistryListItem;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMetadataHandlerTest {
    private static final String QUERY_ID = "queryId";
    private KafkaMetadataHandler kafkaMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private Block partitions;
    private List<String> partitionCols;
    private Constraints constraints;

    private MockedStatic<GlueClient> awsGlueClientBuilder;

    @Mock
    GlueClient glueClient;

    MockConsumer<String, String> consumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        partitions = Mockito.mock(Block.class);
        partitionCols = Mockito.mock(List.class);
        constraints = Mockito.mock(Constraints.class);
        java.util.Map configOptions = com.google.common.collect.ImmutableMap.of(
            "aws.region", "us-west-2",
            "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-NEW",
            "auth_type", KafkaUtils.AuthType.SSL.toString(),
            "secret_manager_kafka_creds_name", "testSecret",
            "kafka_endpoint", "12.207.18.179:9092",
            "certificates_s3_reference", "s3://kafka-connector-test-bucket/kafkafiles/",
            "secrets_manager_secret", "Kafka_afq");

        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        Map<TopicPartition, Long> partitionsStart = new HashMap<>();
        Map<TopicPartition, Long> partitionsEnd = new HashMap<>();

        // max splits per request is 1000. Here we will make 1500 partitions that each have the max records
        // for a single split. we expect 1500 splits to generate, over two requests.
        for (int i = 0; i < 1500; i++) {
            partitionsStart.put(new TopicPartition("testTopic", i), 0L);
            partitionsEnd.put(new TopicPartition("testTopic",  i), com.amazonaws.athena.connectors.kafka.KafkaConstants.MAX_RECORDS_IN_SPLIT - 1L); // keep simple and don't have multiple pieces
        }
        List<PartitionInfo> partitionInfoList = new ArrayList<>(partitionsStart.keySet())
                .stream()
                .map(it -> new PartitionInfo(it.topic(), it.partition(), null, null, null))
                .collect(Collectors.toList());
        consumer.updateBeginningOffsets(partitionsStart);
        consumer.updateEndOffsets(partitionsEnd);
        consumer.updatePartitions("testTopic", partitionInfoList);

        awsGlueClientBuilder = Mockito.mockStatic(GlueClient.class);
        awsGlueClientBuilder.when(()-> GlueClient.create()).thenReturn(glueClient);

        kafkaMetadataHandler = new KafkaMetadataHandler(consumer, configOptions);
    }

    @After
    public void tearDown() {
        blockAllocator.close();
        awsGlueClientBuilder.close();
    }

    @Test
    public void testDoListSchemaNames() {
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class))).thenAnswer(x -> (ListRegistriesResponse.builder()
                .registries((RegistryListItem.builder())
                        .registryName("Asdf")
                        .description("something something {AthenaFederationKafka} something")
                        .build())
                .build()
        ));

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        assertEquals(new ArrayList(com.google.common.collect.ImmutableList.of("Asdf")), new ArrayList(listSchemasResponse.getSchemas()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }


    @Test
    public void testDoGetTable() throws Exception {
        String arn = "defaultarn", schemaName = "defaultschemaname", schemaVersionId = "defaultversionid";
        Long latestSchemaVersion = 123L;
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(arn)
                .schemaName(schemaName)
                .latestSchemaVersion(latestSchemaVersion)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(schemaVersionId)
                .dataFormat("json")
                .schemaDefinition("{\n" +
                        "\t\"topicName\": \"testtable\",\n" +
                        "\t\"message\": {\n" +
                        "\t\t\"dataFormat\": \"json\",\n" +
                        "\t\t\"fields\": [{\n" +
                        "\t\t\t\"name\": \"intcol\",\n" +
                        "\t\t\t\"mapping\": \"intcol\",\n" +
                        "\t\t\t\"type\": \"INTEGER\"\n" +
                        "\t\t}]\n" +
                        "\t}\n" +
                        "}")
                .build();
        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "kafka", new TableName("default", "testtable"), Collections.emptyMap());
        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertEquals(1, getTableResponse.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        String arn = "defaultarn", schemaName = "defaultschemaname", schemaVersionId = "defaultversionid";
        Long latestSchemaVersion = 123L;
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(arn)
                .schemaName(schemaName)
                .latestSchemaVersion(latestSchemaVersion)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(schemaVersionId)
                .dataFormat("json")
                .schemaDefinition("{\n" +
                        "\t\"topicName\": \"testTopic\",\n" +
                        "\t\"message\": {\n" +
                        "\t\t\"dataFormat\": \"json\",\n" +
                        "\t\t\"fields\": [{\n" +
                        "\t\t\t\"name\": \"intcol\",\n" +
                        "\t\t\t\"mapping\": \"intcol\",\n" +
                        "\t\t\t\"type\": \"INTEGER\"\n" +
                        "\t\t}]\n" +
                        "\t}\n" +
                        "}")
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetSplitsRequest request = new GetSplitsRequest(
                federatedIdentity,
                QUERY_ID,
                "kafka",
                new TableName("default", "testTopic"),
                Mockito.mock(Block.class),
                new ArrayList<>(),
                Mockito.mock(Constraints.class),
                null 
        );

        GetSplitsResponse response = kafkaMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(1000, response.getSplits().size());
        assertEquals("1000", response.getContinuationToken());
        request = new GetSplitsRequest(
                federatedIdentity,
                QUERY_ID,
                "kafka",
                new TableName("default", "testTopic"),
                Mockito.mock(Block.class),
                new ArrayList<>(),
                Mockito.mock(Constraints.class),
                response.getContinuationToken()
        );
        response = kafkaMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(500, response.getSplits().size());
        assertNull(response.getContinuationToken());
    }
}
