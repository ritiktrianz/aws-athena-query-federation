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
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.GetRegistryResponse;
import software.amazon.awssdk.services.glue.model.ListRegistriesRequest;
import software.amazon.awssdk.services.glue.model.ListRegistriesResponse;
import software.amazon.awssdk.services.glue.model.RegistryListItem;
import software.amazon.awssdk.services.glue.model.SchemaListItem;

import org.apache.kafka.clients.consumer.MockConsumer;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.athena.connectors.kafka.dto.TopicPartitionPiece;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMetadataHandlerTest {
    private static final String QUERY_ID = "queryId";
    private static final String EARLIEST = "earliest";
    private static final String TEST_TOPIC = "testTopic";
    private static final String TEST_TABLE = "testtable";
    private static final String JSON_FORMAT = "json";
    private static final String DEFAULT_ARN = "defaultarn";
    private static final String SCHEMA_ARN = "arn";
    private static final String DEFAULT_SCHEMA_NAME = "defaultschemaname";
    private static final String TEST_SCHEMA = "TestSchema";
    private static final String TEST_REGISTRY = "TestRegistry";
    private static final String DEFAULT_VERSION_ID = "defaultversionid";
    private static final String ID = "id";
    private static final String NAME = "name";
    private static final Long DEFAULT_LATEST_SCHEMA_VERSION = 123L;
    private static final String KAFKA_CATALOG = "kafka";
    private static final String DEFAULT_SCHEMA = "default";
    private static final String TEST_REGISTRY_DESCRIPTION = "something something {AthenaFederationKafka} something";
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final long START_OFFSET = 0L; 
    
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
        MockitoAnnotations.openMocks(this);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        partitions = Mockito.mock(Block.class);
        partitionCols = Mockito.mock(List.class);
        constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
        Map<String, String> configOptions = Map.of(
            "aws.region", "us-west-2",
            "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-NEW",
            "auth_type", KafkaUtils.AuthType.SSL.toString(),
            "secret_manager_kafka_creds_name", "testSecret",
            "kafka_endpoint","12.207.18.179:9092" ,
            "certificates_s3_reference", "s3://kafka-connector-test-bucket/kafkafiles/",
            "secrets_manager_secret", "Kafka_afq");

        consumer = new MockConsumer<>(EARLIEST);
        Map<TopicPartition, Long> partitionsStart = new HashMap<>();
        Map<TopicPartition, Long> partitionsEnd = new HashMap<>();

        // max splits per request is 1000. Here we will make 1500 partitions that each have the max records
        // for a single split. we expect 1500 splits to generate, over two requests.
        for (int i = 0; i < 1500; i++) {
            partitionsStart.put(new TopicPartition(TEST_TOPIC, i), START_OFFSET);
            partitionsEnd.put(new TopicPartition(TEST_TOPIC,  i), com.amazonaws.athena.connectors.kafka.KafkaConstants.MAX_RECORDS_IN_SPLIT - 1L); // keep simple and don't have multiple pieces
        }
        List<PartitionInfo> partitionInfoList = new ArrayList<>(partitionsStart.keySet())
                .stream()
                .map(it -> new PartitionInfo(it.topic(), it.partition(), null, null, null))
                .collect(Collectors.toList());
        consumer.updateBeginningOffsets(partitionsStart);
        consumer.updateEndOffsets(partitionsEnd);
        consumer.updatePartitions(TEST_TOPIC, partitionInfoList);

        awsGlueClientBuilder = Mockito.mockStatic(GlueClient.class);
        awsGlueClientBuilder.when(GlueClient::create).thenReturn(glueClient);

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
                        .description(TEST_REGISTRY_DESCRIPTION)
                        .build())
                .build()
        ));

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, DEFAULT_SCHEMA);
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
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaName(DEFAULT_SCHEMA_NAME)
                .latestSchemaVersion(DEFAULT_LATEST_SCHEMA_VERSION)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaVersionId(DEFAULT_VERSION_ID)
                .dataFormat(JSON_FORMAT)
                .schemaDefinition("{\n" +
                        "\t\"topicName\": \"" + TEST_TABLE + "\",\n" +
                        "\t\"message\": {\n" +
                        "\t\t\"dataFormat\": \"" + JSON_FORMAT + "\",\n" +
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
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, KAFKA_CATALOG, new TableName(DEFAULT_SCHEMA, TEST_TABLE), Collections.emptyMap());
        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertEquals(1, getTableResponse.getSchema().getFields().size());
    }

    @Test
    public void testDoGetTableWithCaseInsensitiveResolution() throws Exception {
        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class)))
                .thenThrow(new RuntimeException("Schema not found"))
                .thenReturn(GetSchemaResponse.builder()
                        .schemaArn(SCHEMA_ARN)
                        .schemaName(TEST_SCHEMA)
                        .latestSchemaVersion(1L)
                        .build());

        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName(TEST_REGISTRY)
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse =
                software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                        .schemas(SchemaListItem.builder().schemaName(TEST_SCHEMA).build())
                        .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(SCHEMA_ARN)
                .schemaVersionId("1")
                .dataFormat(JSON_FORMAT)
                .schemaDefinition("{\n" +
                        "\t\"topicName\": \"testtable\",\n" +
                        "\t\"message\": {\n" +
                        "\t\t\"dataFormat\": \"json\",\n" +
                        "\t\t\"fields\": [{\n" +
                        "\t\t\t\"name\": \"col1\",\n" +
                        "\t\t\t\"mapping\": \"col1\",\n" +
                        "\t\t\t\"type\": \"STRING\"\n" +
                        "\t\t}]\n" +
                        "\t}\n" +
                        "}")
                .build();
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class)))
                .thenReturn(schemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, TEST_SCHEMA),
                Collections.emptyMap()
        );
        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertEquals(1, getTableResponse.getSchema().getFields().size());
        assertEquals("col1", getTableResponse.getSchema().getFields().get(0).getName());
        assertEquals(TEST_REGISTRY, getTableResponse.getTableName().getSchemaName());
        assertEquals(TEST_SCHEMA, getTableResponse.getTableName().getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithInvalidSchema() throws Exception {
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(SCHEMA_ARN)
                .schemaName("invalid")
                .latestSchemaVersion(1L)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(SCHEMA_ARN)
                .schemaVersionId("1")
                .dataFormat(JSON_FORMAT)
                .schemaDefinition("invalid json")
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(DEFAULT_SCHEMA, "invalid"),
                Collections.emptyMap()
        );
        kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
    }

    @Test
    public void testPieceTopicPartitionSinglePiece() {
        long startOffset = START_OFFSET;
        long endOffset = 5000L; // Less than MAX_RECORDS_IN_SPLIT (10000)

        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);

        assertEquals(1, pieces.size());
        assertEquals(startOffset, pieces.get(0).startOffset);
        assertEquals(endOffset, pieces.get(0).endOffset);
    }

    @Test
    public void testPieceTopicPartitionMultiplePieces() {
        long endOffset = 25000L;

        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(START_OFFSET, endOffset);

        assertEquals(3, pieces.size());

        assertEquals(START_OFFSET, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);

        assertEquals(10001L, pieces.get(1).startOffset);
        assertEquals(20001L, pieces.get(1).endOffset);

        assertEquals(20002L, pieces.get(2).startOffset);
        assertEquals(25000L, pieces.get(2).endOffset);
    }

    @Test
    public void testPieceTopicPartitionWithNonZeroStart() {
        long startOffset = 5000L;
        long endOffset = 35000L;

        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);

        assertEquals(3, pieces.size());

        assertEquals(5000L, pieces.get(0).startOffset);
        assertEquals(15000L, pieces.get(0).endOffset);

        assertEquals(15001L, pieces.get(1).startOffset);
        assertEquals(25001L, pieces.get(1).endOffset);

        assertEquals(25002L, pieces.get(2).startOffset);
        assertEquals(35000L, pieces.get(2).endOffset);
    }

    @Test
    public void testPieceTopicPartitionEdgeCaseMaxRecords() {
        long endOffset = 10000L;

        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(START_OFFSET, endOffset);

        assertEquals(1, pieces.size());
        assertEquals(START_OFFSET, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
    }

    @Test
    public void testPieceTopicPartitionLargePartition() {
        long endOffset = 1_000_000L;

        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(START_OFFSET, endOffset);

        assertEquals(100, pieces.size());

        assertEquals(START_OFFSET, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);

        assertEquals(500050L, pieces.get(50).startOffset);
        assertEquals(510050L, pieces.get(50).endOffset);

        assertEquals(990099L, pieces.get(99).startOffset);
        assertEquals(1000000L, pieces.get(99).endOffset);
    }

    @Test
    public void testDoGetTableWithAvroFormat() throws Exception {
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/avroSchema")
                .schemaName("avroSchema")
                .latestSchemaVersion(1L)
                .build();

        String avroSchemaDefinition = "{\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\",\n" +
                "      \"formatHint\": \"%d\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"formatHint\": \"%s\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/avroSchema")
                .schemaVersionId("1")
                .dataFormat("AVRO")
                .schemaDefinition(avroSchemaDefinition)
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, "avroSchema"),
                Collections.emptyMap()
        );

        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertEquals(2, getTableResponse.getSchema().getFields().size());
        assertEquals(ID, getTableResponse.getSchema().getFields().get(0).getName());
        assertEquals(NAME, getTableResponse.getSchema().getFields().get(1).getName());

        assertEquals(ID, getTableResponse.getSchema().getFields().get(0).getMetadata().get(NAME));
        assertEquals("int", getTableResponse.getSchema().getFields().get(0).getMetadata().get("type"));
        assertEquals("%d", getTableResponse.getSchema().getFields().get(0).getMetadata().get("formatHint"));

        assertEquals(NAME, getTableResponse.getSchema().getFields().get(1).getMetadata().get(NAME));
        assertEquals("string", getTableResponse.getSchema().getFields().get(1).getMetadata().get("type"));
        assertEquals("%s", getTableResponse.getSchema().getFields().get(1).getMetadata().get("formatHint"));

        assertEquals("avro", getTableResponse.getSchema().getCustomMetadata().get("dataFormat"));
        assertEquals(TEST_REGISTRY, getTableResponse.getSchema().getCustomMetadata().get("glueRegistryName"));
        assertEquals("avroSchema", getTableResponse.getSchema().getCustomMetadata().get("glueSchemaName"));
    }

    @Test
    public void testDoGetTableWithProtobufFormat() throws Exception {
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/protobufSchema")
                .schemaName("protobufSchema")
                .latestSchemaVersion(1L)
                .build();

        String protobufSchemaDefinition = "syntax = \"proto3\";\n" +
                "message TestMessage {\n" +
                "  int32 id = 1;\n" +
                "  string name = 2;\n" +
                "  double price = 3;\n" +
                "}";

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/protobufSchema")
                .schemaVersionId("1")
                .dataFormat("PROTOBUF")
                .schemaDefinition(protobufSchemaDefinition)
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, "protobufSchema"),
                Collections.emptyMap()
        );

        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertEquals(3, getTableResponse.getSchema().getFields().size());
        assertEquals(ID, getTableResponse.getSchema().getFields().get(0).getName());
        assertEquals(NAME, getTableResponse.getSchema().getFields().get(1).getName());
        assertEquals("price", getTableResponse.getSchema().getFields().get(2).getName());

        assertEquals("protobuf", getTableResponse.getSchema().getCustomMetadata().get("dataFormat"));
        assertEquals(TEST_REGISTRY, getTableResponse.getSchema().getCustomMetadata().get("glueRegistryName"));
        assertEquals("protobufSchema", getTableResponse.getSchema().getCustomMetadata().get("glueSchemaName"));
    }

    @Test
    public void testDoListTablesWithUnlimitedPageSize() {
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName(TEST_REGISTRY)
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        List<SchemaListItem> schemaItems = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            schemaItems.add(SchemaListItem.builder().schemaName("schema" + i).build());
        }

        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse =
                software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                        .schemas(schemaItems)
                        .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                TEST_REGISTRY,
                null,
                ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE
        );

        ListTablesResponse response = kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);

        assertEquals(50, response.getTables().size());
        assertNull(response.getNextToken());
        
        for (int i = 1; i <= 50; i++) {
            final int schemaIndex = i;
            boolean found = response.getTables().stream()
                    .anyMatch(table -> table.getTableName().equals("schema" + schemaIndex));
            assertTrue("Schema" + schemaIndex + " should be present", found);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testDoListTablesExceedsMaxResults() {
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName(TEST_REGISTRY)
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        List<SchemaListItem> largeSchemaList = new ArrayList<>();
        for (int i = 1; i <= 100001; i++) {
            largeSchemaList.add(SchemaListItem.builder().schemaName("schema" + i).build());
        }

        software.amazon.awssdk.services.glue.model.ListSchemasResponse largeResponse =
                software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                        .schemas(largeSchemaList)
                        .build();

        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(largeResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                TEST_REGISTRY,
                null,
                ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE
        );

        kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        String arn = DEFAULT_ARN;
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(arn)
                .schemaName(DEFAULT_SCHEMA_NAME)
                .latestSchemaVersion(DEFAULT_LATEST_SCHEMA_VERSION)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(DEFAULT_VERSION_ID)
                .dataFormat(JSON_FORMAT)
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
                KAFKA_CATALOG,
                new TableName(DEFAULT_SCHEMA, TEST_TOPIC),
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
                KAFKA_CATALOG,
                new TableName(DEFAULT_SCHEMA, TEST_TOPIC),
                Mockito.mock(Block.class),
                new ArrayList<>(),
                Mockito.mock(Constraints.class),
                response.getContinuationToken()
        );
        response = kafkaMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(500, response.getSplits().size());
        assertNull(response.getContinuationToken());
    }

    @Test
    public void testDoListSchemaNamesWithPagination() {
        ListRegistriesResponse firstPageResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName("Registry1")
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .nextToken("token1")
                .build();

        ListRegistriesResponse secondPageResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName("Registry2")
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(firstPageResponse)
                .thenReturn(secondPageResponse);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, DEFAULT_SCHEMA);
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        List<String> expectedRegistries = Arrays.asList("Registry1", "Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListSchemaNamesWithNullDescription() {
        ListRegistriesResponse response = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName("Registry1")
                                .description(null)
                                .build(),
                        RegistryListItem.builder()
                                .registryName("Registry2")
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(response);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, DEFAULT_SCHEMA);
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        List<String> expectedRegistries = List.of("Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListTablesWithEmptyDescription() {
        GetRegistryResponse getRegistryResponse = GetRegistryResponse.builder()
                .registryName(TEST_REGISTRY)
                .description("")
                .build();

        Mockito.when(glueClient.getRegistry(any(GetRegistryRequest.class)))
                .thenReturn(getRegistryResponse);

        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                        RegistryListItem.builder()
                                .registryName(TEST_REGISTRY)
                                .description(TEST_REGISTRY_DESCRIPTION)
                                .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse =
                software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                        .schemas(SchemaListItem.builder().schemaName(TEST_SCHEMA).build())
                        .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                TEST_REGISTRY,
                TEST_SCHEMA,
                DEFAULT_PAGE_SIZE
        );

        ListTablesResponse response = kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        List<TableName> tables = new ArrayList<>(response.getTables());
        assertEquals(1, tables.size());
        assertEquals(TEST_REGISTRY, tables.get(0).getSchemaName());
        assertEquals(TEST_SCHEMA, tables.get(0).getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithGlueException() throws Exception {
        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class)))
                .thenThrow(new RuntimeException("Glue service unavailable"));

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, TEST_SCHEMA),
                Collections.emptyMap()
        );

        kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
    }

    @Test(expected = RuntimeException.class)
    public void testDoListTablesWithGlueException() {
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenThrow(new RuntimeException("Glue service unavailable"));

        ListTablesRequest listTablesRequest = new ListTablesRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                TEST_REGISTRY,
                null,
                DEFAULT_PAGE_SIZE
        );

        kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
    }

    @Test
    public void testDoGetTableWithEmptyAvroFields() throws Exception {
        // Mock Glue responses for AVRO format with empty fields
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/emptyAvroSchema")
                .schemaName("emptyAvroSchema")
                .latestSchemaVersion(1L)
                .build();

        // Create AVRO schema definition with empty fields array
        String emptyAvroSchemaDefinition = "{\n" +
                "  \"name\": \"EmptyRecord\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": []\n" +
                "}";

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/emptyAvroSchema")
                .schemaVersionId("1")
                .dataFormat("AVRO")
                .schemaDefinition(emptyAvroSchemaDefinition)
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, "emptyAvroSchema"),
                Collections.emptyMap()
        );

        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        // Verify schema has no fields but proper metadata
        assertEquals(0, getTableResponse.getSchema().getFields().size());
        assertEquals("avro", getTableResponse.getSchema().getCustomMetadata().get("dataFormat"));
        assertEquals(TEST_REGISTRY, getTableResponse.getSchema().getCustomMetadata().get("glueRegistryName"));
        assertEquals("emptyAvroSchema", getTableResponse.getSchema().getCustomMetadata().get("glueSchemaName"));
    }

    @Test
    public void testDoGetTableWithEmptyProtobufFields() throws Exception {
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/emptyProtobufSchema")
                .schemaName("emptyProtobufSchema")
                .latestSchemaVersion(1L)
                .build();

        // Create PROTOBUF schema definition with no fields
        String emptyProtobufSchemaDefinition = "syntax = \"proto3\";\n" +
                "message EmptyMessage {\n" +
                "}";

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn:aws:glue:us-west-2:123456789101:schema/TestRegistry/emptyProtobufSchema")
                .schemaVersionId("1")
                .dataFormat("PROTOBUF")
                .schemaDefinition(emptyProtobufSchemaDefinition)
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
                federatedIdentity,
                QUERY_ID,
                KAFKA_CATALOG,
                new TableName(TEST_REGISTRY, "emptyProtobufSchema"),
                Collections.emptyMap()
        );

        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertEquals(0, getTableResponse.getSchema().getFields().size());
        assertEquals("protobuf", getTableResponse.getSchema().getCustomMetadata().get("dataFormat"));
        assertEquals(TEST_REGISTRY, getTableResponse.getSchema().getCustomMetadata().get("glueRegistryName"));
        assertEquals("emptyProtobufSchema", getTableResponse.getSchema().getCustomMetadata().get("glueSchemaName"));
    }
}
