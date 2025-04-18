/*-
 * #%L
 * athena-example
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.elasticsearch.qpt.ElasticsearchQueryPassthrough;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for providing Athena with actual rows level data from your Elasticsearch instance. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ElasticsearchMetadataHandler.
 */
public class ElasticsearchRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "elasticsearch";

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private static final String AUTO_DISCOVER_ENDPOINT = "auto_discover_endpoint";

    // Env. variable that holds the query timeout period for the Search queries.
    private static final String QUERY_TIMEOUT_SEARCH = "query_timeout_search";
    // Env. variable that holds the scroll timeout for the Search queries.
    private static final String SCROLL_TIMEOUT = "query_scroll_timeout";

    private final long queryTimeout;
    private final long scrollTimeout;

    // Pagination batch size (100 documents).
    private static final int QUERY_BATCH_SIZE = 100;

    private final AwsRestHighLevelClientFactory clientFactory;
    private final ElasticsearchTypeUtils typeUtils;
    private final ElasticsearchQueryPassthrough queryPassthrough = new ElasticsearchQueryPassthrough();

    public ElasticsearchRecordHandler(Map<String, String> configOptions)
    {
        super(S3Client.create(), SecretsManagerClient.create(),
                AthenaClient.create(), SOURCE_TYPE, configOptions);

        this.typeUtils = new ElasticsearchTypeUtils();
        this.clientFactory = new AwsRestHighLevelClientFactory(configOptions.getOrDefault(AUTO_DISCOVER_ENDPOINT, "").equalsIgnoreCase("true"));
        this.queryTimeout = Long.parseLong(configOptions.getOrDefault(QUERY_TIMEOUT_SEARCH, "720"));
        this.scrollTimeout = Long.parseLong(configOptions.getOrDefault(SCROLL_TIMEOUT, "60"));
    }

    @VisibleForTesting
    protected ElasticsearchRecordHandler(
        S3Client amazonS3,
        SecretsManagerClient secretsManager,
        AthenaClient amazonAthena,
        AwsRestHighLevelClientFactory clientFactory,
        long queryTimeout,
        long scrollTimeout,
        Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);

        this.typeUtils = new ElasticsearchTypeUtils();
        this.clientFactory = clientFactory;
        this.queryTimeout = queryTimeout;
        this.scrollTimeout = scrollTimeout;
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws RuntimeException when an error occurs while attempting to send the query, or the query timed out.
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker)
            throws RuntimeException
    {
        String domain;
        QueryBuilder query;
        String index;
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            Map<String, String> qptArgs = recordsRequest.getConstraints().getQueryPassthroughArguments();
            queryPassthrough.verify(qptArgs);
            domain = qptArgs.get(ElasticsearchQueryPassthrough.SCHEMA);
            index = qptArgs.get(ElasticsearchQueryPassthrough.INDEX);
            query = QueryBuilders.wrapperQuery(qptArgs.get(ElasticsearchQueryPassthrough.QUERY));
        }
        else {
            domain = recordsRequest.getTableName().getSchemaName();
            index = recordsRequest.getSplit().getProperty(ElasticsearchMetadataHandler.INDEX_KEY);
            query = ElasticsearchQueryUtils.getQuery(recordsRequest.getConstraints());
        }

        String endpoint = recordsRequest.getSplit().getProperty(domain);
        String shard = recordsRequest.getSplit().getProperty(ElasticsearchMetadataHandler.SHARD_KEY);
        String username = recordsRequest.getSplit().getProperty(ElasticsearchMetadataHandler.SECRET_USERNAME);
        String password = recordsRequest.getSplit().getProperty(ElasticsearchMetadataHandler.SECRET_PASSWORD);
        boolean useSecret = StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password);
        logger.info("readWithConstraint - enter - Domain: {}, Index: {}, Mapping: {}, Query: {}",
                domain, index,
                recordsRequest.getSchema(), query);
        long numRows = 0;

        if (queryStatusChecker.isQueryRunning()) {
            AwsRestHighLevelClient client = useSecret ? clientFactory.getOrCreateClient(endpoint, username, password) : clientFactory.getOrCreateClient(endpoint);
            try {
                // Create field extractors for all data types in the schema.
                GeneratedRowWriter rowWriter = createFieldExtractors(recordsRequest);

                // Create a new search-source injected with the projection, predicate, and the pagination batch size.
                SearchSourceBuilder searchSource = new SearchSourceBuilder()
                        .size(QUERY_BATCH_SIZE)
                        .timeout(new TimeValue(queryTimeout, TimeUnit.SECONDS))
                        .fetchSource(ElasticsearchQueryUtils.getProjection(recordsRequest.getSchema()))
                        .query(query);

                //init scroll
                Scroll scroll = new Scroll(TimeValue.timeValueSeconds(this.scrollTimeout));
                // Create a new search-request for the specified index.
                SearchRequest searchRequest = new SearchRequest(index)
                        .preference(shard)
                        .scroll(scroll)
                        .source(searchSource.from(0));

                //Read the returned scroll id, which points to the search context that's being kept alive and will be needed in the following search scroll call
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

                while (searchResponse.getHits() != null
                        && searchResponse.getHits().getHits() != null
                        && searchResponse.getHits().getHits().length > 0
                        && queryStatusChecker.isQueryRunning()) {
                    Iterator<SearchHit> finalIterator = searchResponse.getHits().iterator();
                    while (finalIterator.hasNext() && queryStatusChecker.isQueryRunning()) {
                        ++numRows;
                        spiller.writeRows((Block block, int rowNum) ->
                                rowWriter.writeRow(block, rowNum, client.getDocument(finalIterator.next())) ? 1 : 0);
                    }

                    //prep for next hits and keep track of scroll id.
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId()).scroll(scroll);
                    searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                    if (searchResponse.isTimedOut()) {
                        throw new AthenaConnectorException("Request for index (" + index + ") " + shard + " timed out.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString()).build());
                    }
                }

                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(searchResponse.getScrollId());
                client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            }
            catch (IOException error) {
                throw new AthenaConnectorException("Error sending search query: " + error.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).errorMessage(error.getMessage()).build());
            }
        }

        logger.info("readWithConstraint: numRows[{}]", numRows);
    }

    /**
     * Creates field extractors to aid in extracting values from retrieved documents. Method makeExtractor()
     * is used for creating the extractors for simple data types (e.g. INT, BIGINT, etc...) Complex data types such as
     * LIST and STRUCT, however require the makeFactory() method to create the extractors.
     * @param recordsRequest Details of the read request that include the constraints and list of fields in the schema.
     * @return GeneratedRowWriter which includes all field extractors used for processing of retrieved documents.
     */
    private GeneratedRowWriter createFieldExtractors(ReadRecordsRequest recordsRequest)
    {
        GeneratedRowWriter.RowWriterBuilder builder =
                GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        for (Field field : recordsRequest.getSchema().getFields()) {
            Extractor extractor = typeUtils.makeExtractor(field);
            if (extractor != null) {
                // Simple data types (e.g. INT, BIGINT, etc...)
                builder.withExtractor(field.getName(), extractor);
            }
            else {
                // Complex data types (e.g. LIST, STRUCT)
                builder.withFieldWriterFactory(field.getName(), typeUtils.makeFactory(field));
            }
        }

        return builder.build();
    }

    /**
     * @return value used for pagination batch size.
     */
    @VisibleForTesting
    protected int getQueryBatchSize()
    {
        return QUERY_BATCH_SIZE;
    }
}
