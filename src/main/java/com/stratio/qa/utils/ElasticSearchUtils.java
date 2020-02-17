/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.qa.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class ElasticSearchUtils extends RestClient.FailureListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchUtil.class);

    private String es_host;

    private int es_native_port;

    private RestHighLevelClient client;

    private Settings settings;

    /**
     * Default constructor.
     */
    public ElasticSearchUtils() {
        this.es_host = System.getProperty("ES_NODE", "127.0.0.1");
        this.es_native_port = Integer.valueOf(System.getProperty("ES_NATIVE_PORT", "9200"));
    }

    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Set settings about ES connector.
     *
     * @param settings : LinkedHashMap with all the settings about ES connection
     */
    public void setSettings(LinkedHashMap<String, Object> settings) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().toString());
        }
        this.settings = builder.build();
    }

    public void setHost(String host) {
        this.es_host = host;
    }

    public void setNativePort(Integer port) {
        this.es_native_port = port;
    }

    /**
     * Connect to ES.
     */
    public void connect(String keyStorePath, String  keyStorePassword, String  trustorePath, String trustorePassword) throws SSLException {
        HttpHost httpHost = new HttpHost(this.es_host, this.es_native_port, "https");
        SSLContext sslContext = initializeSSLContext(keyStorePath, keyStorePassword, trustorePath, trustorePassword);

        this.client = new RestHighLevelClient(RestClient.builder(httpHost).setFailureListener(this).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext)));
    }

    public void connect() {

        HttpHost httpHost = new HttpHost(this.es_host, this.es_native_port, "http");
        this.client = new RestHighLevelClient(RestClient.builder(httpHost).setFailureListener(this));
    }

    /**
     * Get ES client(Connected previously).
     *
     * @return es client
     */
    public RestHighLevelClient getClient() {
        return this.client;
    }

    /**
     * Create an ES Index.
     *
     * @param indexName
     * @return true if the index has been created and false if the index has not been created.
     * @throws ElasticsearchException
     */
    public boolean createSingleIndex(String indexName) {
        CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
        try {
            this.client.indices().create(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error creating index: " + indexName);

        }
        return indexExists(indexName);
    }

    /**
     * Drop an ES Index
     *
     * @param indexName
     * @return true if the index exists
     * @throws ElasticsearchException
     */
    public boolean dropSingleIndex(String indexName) throws ElasticsearchException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        try {
            this.client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error dropping index: " + indexName);
        }
        return indexExists(indexName);
    }

    public boolean dropAllIndexes() {

        boolean result = true;

        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsResponse response;

        try {
            response  = client.indices().getMapping(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error getting indices names");

        }

        Map<String, MappingMetaData> mappings = response.mappings();

        for (String indexName : mappings.keySet()) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            try {
                this.client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new ElasticsearchException("Error deleting index: " + indexName);
            }
            result = indexExists(indexName);
        }
        return result;
    }

    /**
     * Check if an index exists in ES
     *
     * @param indexName
     * @return true if the index exists or false if the index does not exits.
     */
    public boolean indexExists(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);

            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error checking if index " + indexName + "exists");
        }
    }

    /**
     * Create a mapping over an index
     *
     * @param indexName
     * @param mappingSource the data that has to be inserted in the mapping.
     */
    public void createMapping(String indexName, ArrayList<XContentBuilder> mappingSource) {
        if (!this.indexExists(indexName)) {
            if (!createSingleIndex(indexName)) {
                throw new ElasticsearchException("Failed to create " + indexName + " index.");
            }
        }

        //If the index does not exists, it will be created without options
        BulkRequest bulkRequest = new BulkRequest();

        for (int i = 0; i < mappingSource.size(); i++) {
            int aux = i + 1;

            bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(aux)).source(mappingSource.get(i)));
        }
        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error in bulk request");
        }
    }

    /**
     * Check if a mapping exists in an expecific index.
     *
     * @param indexName
     * @param mappingName
     * @return true if the mapping exists and false in other case
     */
    public boolean existsMapping(String indexName, String mappingName) {

        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsRequest indices = request.indices(indexName);
        GetMappingsResponse getMappingsResponse;
        try {
            getMappingsResponse = client.indices().getMapping(indices, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error getting mapping " + mappingName);
        }
        return getMappingsResponse.mappings().get(mappingName) != null;
    }

    /**
     * Simulate a SELET * FROM index.mapping WHERE (One simple filter)
     *
     * @param indexName
     * @param columnName
     * @param value
     * @param filterType  [equals, gt, gte, lt, lte]
     * @return ArrayList with all the rows(One element of the ArrayList is a JSON document)
     * @throws Exception
     */
    public List<JSONObject> searchSimpleFilterElasticsearchQuery(String indexName, String columnName, Object value, String filterType) throws Exception {
        List<JSONObject> resultsJSON = new ArrayList<JSONObject>();
        QueryBuilder query;
        switch (filterType) {
            case "equals":
                query = QueryBuilders.termQuery(columnName, value);
                break;
            case "gt":
                query = QueryBuilders.rangeQuery(columnName).gt(value);
                break;
            case "gte":
                query = QueryBuilders.rangeQuery(columnName).gte(value);
                break;
            case "lt":
                query = QueryBuilders.rangeQuery(columnName).lt(value);
                break;
            case "lte":
                query = QueryBuilders.rangeQuery(columnName).lte(value);
                break;
            default:
                throw new Exception("Filter not implemented in the library");
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest().indices(indexName).source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            resultsJSON.add(new JSONObject(hit.getSourceAsString()));
        }
        return resultsJSON;
    }

    /**
     * Indexes a document.
     *
     * @param indexName
     * @param id          unique identifier of the document
     * @param document
     * @throws Exception
     */
    public void indexDocument(String indexName, String id, XContentBuilder document) {
        IndexRequest request = new IndexRequest(indexName).id(id).source(document);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("Error indexing document");
        }
    }

    /**
     * Deletes a document by its id.
     *
     * @param indexName
     * @param id
     */
    public void deleteDocument(String indexName, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, id);
        try {

            client.delete(deleteRequest, RequestOptions.DEFAULT);

        } catch (IOException e) {
            throw new ElasticsearchException("Error deleting document");
        }
    }

    private static SSLContext initializeSSLContext(String keyStore, String keyStorePass, String truststore, String truststorePass) throws SSLException {
        try {

            Path keyStorePath = Paths.get(keyStore);
            Path truststorePath = Paths.get(truststore);
            LOGGER.info("Getting the keystore path which is {} also getting truststore path {}", keyStorePath, truststorePath);

            SSLContext sc = SSLContext.getInstance("TLSv1.2");

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream is = Files.newInputStream(keyStorePath)) {
                ks.load(is, keyStorePass.toCharArray());
            }

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(ks, keyStorePass.toCharArray());


            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream is = Files.newInputStream(truststorePath)) {
                ts.load(is, truststorePass.toCharArray());
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(ts);

            sc.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new java.security.SecureRandom());
            return sc;
        } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
            throw new SSLException("Cannot initialize SSL Context ", e);
        }
    }
}
