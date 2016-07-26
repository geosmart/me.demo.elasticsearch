package me.demo.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Created by geosmart on 2016/7/25.
 *
 * the final actionGet() call on the operations is caused by the asynchronous nature of
 * Elasticsearch and is not related to the HTTP operation. Each operation returns a Future that
 * provides access to the result once it is available.
 */
@Service
public class ESService {
    private static final Logger log = LoggerFactory.getLogger(ESService.class);

    @Value("${elasticsearch.indexName}")
    private String indexName;

    @Value("${elasticsearch.typeName}")
    private String typeName;

    @Inject
    @Qualifier("esClient")
    private Client esClient;

    @Inject
    @Qualifier("bulkProcessor")
    private BulkProcessor bulkProcessor;


    /**
     * 新建Index（database）
     */
    public void createIndex(String indexName, Settings.Builder settings) {
        IndicesAdminClient indicesAdminClient = esClient.admin().indices();
        settings = settings == null ? getDefaultIndexSettings() : settings;
        indicesAdminClient.prepareCreate(indexName).setSettings(settings).execute().actionGet();
    }

    /**
     * 插入document（indexAPI，单条插入）
     */
    public void createDocument(String index, String type, List<Object> docs) {
        for (Object doc : docs) {
            String json = JSON.toJSONString(doc);
            esClient.prepareIndex(index, type).setSource(json).execute().actionGet();
        }
    }

    /**
     * 插入document（bulkRequest，批量插入）
     */
    public void createDocument_bulkRequest(String index, String type, List<Object> docs) {
        BulkRequestBuilder bulkRequestBuilder = esClient.prepareBulk();
        for (Object doc : docs) {
            String json = JSON.toJSONString(doc);
            bulkRequestBuilder.add(esClient.prepareIndex(index, type).setSource(json));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            log.debug("hasFailures：{}", bulkResponse.buildFailureMessage());
        }
    }


    /**
     * 插入document（bulkProcess，批量插入-多线程/自动监听等特性）
     */
    public void createDocument_bulkProcess(String index, String type, List<Object> docs) {
        for (Object doc : docs) {
            String json = JSON.toJSONString(doc);
            bulkProcessor.add(new IndexRequest(index, type).source(json));
        }
    }

    /**
     * 删除document
     */
    public void deleteDocument(String index, String type, String id) {
        DeleteResponse rsp = new DeleteRequestBuilder(esClient, DeleteAction.INSTANCE)
                .setIndex(index).setType(type).setId(id)
                .execute()
                .actionGet();
    }


    /**
     * 删除Index
     */
    public void deleteIndex(String index) {
        boolean indexExists = esClient.admin().indices().prepareExists(index).execute().actionGet().isExists();
        if (indexExists) {
            esClient.admin().indices().prepareDelete(index).execute().actionGet();
        }
    }


    public void deleteByQuery(String index, String type) {
        //TODO
        SearchResponse allHits = esClient.prepareSearch(index)
                .addFields("title", "category")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
    }

    public Settings.Builder getDefaultIndexSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", "4");
        settings.put("number_of_replicas", "2");
        return Settings.builder().put(settings);
    }

    public void getIndexSettings(String... indexes) {
        GetSettingsResponse response = esClient.admin().indices()
                .prepareGetSettings(indexes).get();
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        }
    }


}
