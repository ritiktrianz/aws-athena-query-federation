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

    private static final String CATALOG_NAME = "kafka";
    private static final String TEST_REGISTRY_NAME = "TestRegistry";
    private static final String TEST_REGISTRY_DESCRIPTION = "something something {AthenaFederationKafka} something";
    private static final String NEXT_TOKEN_VALUE = "next-token-123";
    private static final String EXISTING_TOKEN_VALUE = "existing-token";
    private static final String NON_EXISTENT_REGISTRY = "NonExistentRegistry";
    private static final String SCHEMA_1 = "schema1";
    private static final String SCHEMA_2 = "schema2";
    private static final String SCHEMA_3 = "schema3";
    private static final String SCHEMA_4 = "schema4";
    private static final String SCHEMA_5 = "schema5";
    private static final int MAX_RESULTS_PLUS_ONE = 100001;
    private static final int DEFAULT_PAGE_SIZE = 10;

    @Test
    public void testDoListSchemaNames() {
        // Test single page of registries
        ListRegistriesResponse singlePageResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry1")
                        .description("something something {AthenaFederationKafka} something")
                        .build(),
                    RegistryListItem.builder()
                        .registryName("Registry2")
                        .description("not a kafka registry")
                        .build(),
                    RegistryListItem.builder()
                        .registryName("Registry3")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(singlePageResponse);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        // Should only include registries with the AthenaFederationKafka marker
        List<String> expectedRegistries = Arrays.asList("Registry1", "Registry3");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListSchemaNamesWithPagination() {
        // Test multiple pages of registries
        ListRegistriesResponse firstPageResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry1")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .nextToken("token1")
                .build();

        ListRegistriesResponse secondPageResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry2")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(firstPageResponse)
                .thenReturn(secondPageResponse);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        List<String> expectedRegistries = Arrays.asList("Registry1", "Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListSchemaNamesWithNullDescription() {
        // Test registry with null description
        ListRegistriesResponse response = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry1")
                        .description(null)
                        .build(),
                    RegistryListItem.builder()
                        .registryName("Registry2")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(response);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        // Should only include registry with valid marker
        List<String> expectedRegistries = Arrays.asList("Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListSchemaNamesWithEmptyDescription() {
        // Test registry with empty description
        ListRegistriesResponse response = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry1")
                        .description("")
                        .build(),
                    RegistryListItem.builder()
                        .registryName("Registry2")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(response);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        // Should only include registry with valid marker
        List<String> expectedRegistries = Arrays.asList("Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListSchemaNamesWithInvalidMarker() {
        // Test registry with invalid marker
        ListRegistriesResponse response = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("Registry1")
                        .description("something something {InvalidMarker} something")
                        .build(),
                    RegistryListItem.builder()
                        .registryName("Registry2")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(response);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        // Should only include registry with valid marker
        List<String> expectedRegistries = Arrays.asList("Registry2");
        assertEquals(new ArrayList<>(expectedRegistries), new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test
    public void testDoListTablesWithInvalidMarker() {
        // Test registry with invalid marker
        GetRegistryResponse getRegistryResponse = GetRegistryResponse.builder()
                .registryName("TestRegistry")
                .description("something something {InvalidMarker} something")
                .build();

        Mockito.when(glueClient.getRegistry(any(GetRegistryRequest.class)))
                .thenReturn(getRegistryResponse);

        // Mock case-insensitive registry lookup
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("TestRegistry")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        // Mock schema listing
        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse = 
            software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                .schemas(SchemaListItem.builder().schemaName("TestSchema").build())
                .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
            federatedIdentity, 
            QUERY_ID, 
            "kafka", 
            "TestRegistry", 
            "TestSchema",
            DEFAULT_PAGE_SIZE
        );

        ListTablesResponse response = kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        List<TableName> tables = new ArrayList<>(response.getTables());
        assertEquals(1, tables.size());
        assertEquals("TestRegistry", tables.get(0).getSchemaName());
        assertEquals("TestSchema", tables.get(0).getTableName());
    }

    @Test
    public void testDoListTablesWithNullDescription() {
        // Test registry with null description
        GetRegistryResponse getRegistryResponse = GetRegistryResponse.builder()
                .registryName("TestRegistry")
                .description(null)
                .build();

        Mockito.when(glueClient.getRegistry(any(GetRegistryRequest.class)))
                .thenReturn(getRegistryResponse);

        // Mock case-insensitive registry lookup
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("TestRegistry")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        // Mock schema listing
        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse = 
            software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                .schemas(SchemaListItem.builder().schemaName("TestSchema").build())
                .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
            federatedIdentity, 
            QUERY_ID, 
            "kafka", 
            "TestRegistry", 
            "TestSchema",
            DEFAULT_PAGE_SIZE
        );

        ListTablesResponse response = kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        List<TableName> tables = new ArrayList<>(response.getTables());
        assertEquals(1, tables.size());
        assertEquals("TestRegistry", tables.get(0).getSchemaName());
        assertEquals("TestSchema", tables.get(0).getTableName());
    }

    @Test
    public void testDoListTablesWithEmptyDescription() {
        // Test registry with empty description
        GetRegistryResponse getRegistryResponse = GetRegistryResponse.builder()
                .registryName("TestRegistry")
                .description("")
                .build();

        Mockito.when(glueClient.getRegistry(any(GetRegistryRequest.class)))
                .thenReturn(getRegistryResponse);

        // Mock case-insensitive registry lookup
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("TestRegistry")
                        .description("something something {AthenaFederationKafka} something")
                        .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        // Mock schema listing
        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse = 
            software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                .schemas(SchemaListItem.builder().schemaName("TestSchema").build())
                .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        ListTablesRequest listTablesRequest = new ListTablesRequest(
            federatedIdentity, 
            QUERY_ID, 
            "kafka", 
            "TestRegistry", 
            "TestSchema",
            DEFAULT_PAGE_SIZE
        );

        ListTablesResponse response = kafkaMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        List<TableName> tables = new ArrayList<>(response.getTables());
        assertEquals(1, tables.size());
        assertEquals("TestRegistry", tables.get(0).getSchemaName());
        assertEquals("TestSchema", tables.get(0).getTableName());
    }

    @Test
    public void testDoListSchemaNamesWithEmptyResponse() {
        // Test when no registries are returned
        ListRegistriesResponse emptyResponse = ListRegistriesResponse.builder()
                .registries(Collections.emptyList())
                .build();

        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(emptyResponse);

        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);

        assertTrue(listSchemasResponse.getSchemas().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse listSchemasResponse = kafkaMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }


    @Test
    public void testDoGetTableWithJsonFormat() throws Exception {
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
                        "\t\t}, {\n" +
                        "\t\t\t\"name\": \"stringcol\",\n" +
                        "\t\t\t\"mapping\": \"stringcol\",\n" +
                        "\t\t\t\"type\": \"STRING\",\n" +
                        "\t\t\t\"formatHint\": \"%s\"\n" +
                        "\t\t}]\n" +
                        "\t}\n" +
                        "}")
                .build();
        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);
        
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "kafka", new TableName("default", "testtable"), Collections.emptyMap());
        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        
        // Verify schema fields
        assertEquals(2, getTableResponse.getSchema().getFields().size());
        assertEquals("intcol", getTableResponse.getSchema().getFields().get(0).getName());
        assertEquals("stringcol", getTableResponse.getSchema().getFields().get(1).getName());
        
        // Verify field metadata
        assertEquals("intcol", getTableResponse.getSchema().getFields().get(0).getMetadata().get("mapping"));
        assertEquals("INTEGER", getTableResponse.getSchema().getFields().get(0).getMetadata().get("type"));
        assertEquals("%s", getTableResponse.getSchema().getFields().get(1).getMetadata().get("formatHint"));
        
        // Verify schema metadata
        assertEquals("json", getTableResponse.getSchema().getCustomMetadata().get("dataFormat"));
        assertEquals("default", getTableResponse.getSchema().getCustomMetadata().get("glueRegistryName"));
        assertEquals("testtable", getTableResponse.getSchema().getCustomMetadata().get("glueSchemaName"));
    }

    @Test
    public void testDoGetTableWithCaseInsensitiveResolution() throws Exception {
        // First call to getSchema fails, triggering case-insensitive resolution
        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class)))
                .thenThrow(new RuntimeException("Schema not found"))
                .thenReturn(GetSchemaResponse.builder()
                        .schemaArn("arn")
                        .schemaName("TestSchema")
                        .latestSchemaVersion(1L)
                        .build());

        // Mock case-insensitive registry lookup
        ListRegistriesResponse registriesResponse = ListRegistriesResponse.builder()
                .registries(
                    RegistryListItem.builder()
                        .registryName("TestRegistry")
                        .description(TEST_REGISTRY_DESCRIPTION)
                        .build()
                )
                .build();
        Mockito.when(glueClient.listRegistries(any(ListRegistriesRequest.class)))
                .thenReturn(registriesResponse);

        // Mock case-insensitive schema lookup
        software.amazon.awssdk.services.glue.model.ListSchemasResponse schemasResponse = 
            software.amazon.awssdk.services.glue.model.ListSchemasResponse.builder()
                .schemas(SchemaListItem.builder().schemaName("TestSchema").build())
                .build();
        Mockito.when(glueClient.listSchemas(any(software.amazon.awssdk.services.glue.model.ListSchemasRequest.class)))
                .thenReturn(schemasResponse);

        // Mock schema version response
        GetSchemaVersionResponse schemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn")
                .schemaVersionId("1")
                .dataFormat("json")
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

        // Test with mismatched case
        GetTableRequest getTableRequest = new GetTableRequest(
            federatedIdentity, 
            QUERY_ID, 
            "kafka", 
            new TableName("testregistry", "testschema"), 
            Collections.emptyMap()
        );
        GetTableResponse getTableResponse = kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        // Verify the response
        assertEquals(1, getTableResponse.getSchema().getFields().size());
        assertEquals("col1", getTableResponse.getSchema().getFields().get(0).getName());
        assertEquals("TestRegistry", getTableResponse.getTableName().getSchemaName());
        assertEquals("TestSchema", getTableResponse.getTableName().getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithInvalidSchema() throws Exception {
        // Mock schema response with invalid schema definition
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn("arn")
                .schemaName("invalid")
                .latestSchemaVersion(1L)
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn("arn")
                .schemaVersionId("1")
                .dataFormat("json")
                .schemaDefinition("invalid json")
                .build();

        Mockito.when(glueClient.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse);
        Mockito.when(glueClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getSchemaVersionResponse);

        GetTableRequest getTableRequest = new GetTableRequest(
            federatedIdentity, 
            QUERY_ID, 
            "kafka", 
            new TableName("default", "invalid"), 
            Collections.emptyMap()
        );
        kafkaMetadataHandler.doGetTable(blockAllocator, getTableRequest);
    }

    @Test
    public void testPieceTopicPartitionSinglePiece() {
        // Test case where partition fits in a single piece
        long startOffset = 0L;
        long endOffset = 5000L; // Less than MAX_RECORDS_IN_SPLIT (10000)
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        assertEquals(1, pieces.size());
        assertEquals(startOffset, pieces.get(0).startOffset);
        assertEquals(endOffset, pieces.get(0).endOffset);
    }

    @Test
    public void testPieceTopicPartitionMultiplePieces() {
        // Test case where partition needs multiple pieces
        long startOffset = 0L;
        long endOffset = 25000L; // More than MAX_RECORDS_IN_SPLIT (10000)
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        // Should create 3 pieces: 0-10000, 10001-20001, 20002-25000
        assertEquals(3, pieces.size());
        
        // First piece
        assertEquals(0L, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
        
        // Second piece
        assertEquals(10001L, pieces.get(1).startOffset);
        assertEquals(20001L, pieces.get(1).endOffset);
        
        // Third piece
        assertEquals(20002L, pieces.get(2).startOffset);
        assertEquals(25000L, pieces.get(2).endOffset);
    }

    @Test
    public void testPieceTopicPartitionWithNonZeroStart() {
        // Test case with non-zero start offset
        long startOffset = 5000L;
        long endOffset = 35000L;
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        // Should create 3 pieces
        assertEquals(3, pieces.size());
        
        // First piece: 5000-15000
        assertEquals(5000L, pieces.get(0).startOffset);
        assertEquals(15000L, pieces.get(0).endOffset);
        
        // Second piece: 15001-25001
        assertEquals(15001L, pieces.get(1).startOffset);
        assertEquals(25001L, pieces.get(1).endOffset);
        
        // Third piece: 25002-35000
        assertEquals(25002L, pieces.get(2).startOffset);
        assertEquals(35000L, pieces.get(2).endOffset);
    }

    @Test
    public void testPieceTopicPartitionEdgeCaseMaxRecords() {
        // Test case where partition is exactly MAX_RECORDS_IN_SPLIT
        long startOffset = 0L;
        long endOffset = 10000L; // Exactly MAX_RECORDS_IN_SPLIT
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        assertEquals(1, pieces.size());
        assertEquals(0L, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
    }

    @Test
    public void testPieceTopicPartitionEdgeCaseMaxRecordsPlusOne() {
        // Test case where partition is MAX_RECORDS_IN_SPLIT + 1
        long startOffset = 0L;
        long endOffset = 10001L;
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        assertEquals(2, pieces.size());
        
        // First piece: 0-10000
        assertEquals(0L, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
        
        // Second piece: 10001-10001
        assertEquals(10001L, pieces.get(1).startOffset);
        assertEquals(10001L, pieces.get(1).endOffset);
    }

    @Test
    public void testPieceTopicPartitionLargePartition() {
        // Test case with a very large partition
        long startOffset = 0L;
        long endOffset = 1_000_000L;
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        // Should create 100 pieces
        assertEquals(100, pieces.size());
        
        // Verify first piece
        assertEquals(0L, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
        
        // Verify middle piece (50th)
        assertEquals(500050L, pieces.get(50).startOffset);
        assertEquals(510050L, pieces.get(50).endOffset);
        
        // Verify last piece
        assertEquals(990099L, pieces.get(99).startOffset);
        assertEquals(1000000L, pieces.get(99).endOffset);
    }

    @Test
    public void testPieceTopicPartitionZeroLength() {
        // Test case with zero length partition
        long startOffset = 1000L;
        long endOffset = 1000L;
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        assertEquals(1, pieces.size());
        assertEquals(1000L, pieces.get(0).startOffset);
        assertEquals(1000L, pieces.get(0).endOffset);
    }

    @Test
    public void testPieceTopicPartitionWithExactMultiple() {
        // Test case where total records is an exact multiple of MAX_RECORDS_IN_SPLIT
        long startOffset = 0L;
        long endOffset = 30000L; // Exactly 3 * MAX_RECORDS_IN_SPLIT
        
        List<TopicPartitionPiece> pieces = kafkaMetadataHandler.pieceTopicPartition(startOffset, endOffset);
        
        assertEquals(3, pieces.size());
        
        // First piece: 0-10000
        assertEquals(0L, pieces.get(0).startOffset);
        assertEquals(10000L, pieces.get(0).endOffset);
        
        // Second piece: 10001-20001
        assertEquals(10001L, pieces.get(1).startOffset);
        assertEquals(20001L, pieces.get(1).endOffset);
        
        // Third piece: 20002-30000
        assertEquals(20002L, pieces.get(2).startOffset);
        assertEquals(30000L, pieces.get(2).endOffset);
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
