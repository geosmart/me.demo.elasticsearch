package me.demo.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.commons.lang3.StringUtils;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

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
     * 从CSV导入数据
     */
    public void importDocFromCSV(String csvFilePath) {
        String line = "";
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {

            while ((line = br.readLine()) != null) {
                createDocument_bulkProcess(indexName, typeName, line);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据Event_LOG_ID相同的数据去重
     * 获取Event_LOG_ID重复的ID-->删除重复数据
     */
    public void removeDuplicateDoc(String field) {
        List<String> ids = queryDuplicateDoc(indexName, typeName, field);
        for (String id : ids) {
            deleteDocument(indexName, typeName, id);
    }
    }

    /**
     * 查询需去重的Document的ID
     *
     * @param index 索引
     */
    public List<String> queryDuplicateDoc(String index, String type, String identifyField) {
        List<String> docIds = new ArrayList<String>();
        AbstractAggregationBuilder aggregation = AggregationBuilders.terms("duplicateCount").field(identifyField).minDocCount(2).subAggregation(AggregationBuilders.topHits("duplicateDocuments"));

        SearchResponse sr = esClient.prepareSearch().setQuery(matchAllQuery()).setSize(0).addAggregation(aggregation).execute().actionGet();
        if (sr.getAggregations() != null) {
            Terms agg = sr.getAggregations().get("duplicateCount");
            for (Terms.Bucket entry : agg.getBuckets()) {
                // bucket key
                String key = (String) entry.getKey();
                long docCount = entry.getDocCount();
                log.info("key [{}], doc_count [{}]", key, docCount);

                //ask for top_hits for each bucket
                SearchHit[] hits = ((TopHits) entry.getAggregations().get("duplicateDocuments")).getHits().getHits();
                //reserve first doc,return duplicate docId
                for (int i = 1; i < hits.length; i++) {
                    SearchHit hit = hits[i];
                    docIds.add(hit.getId());
                    log.debug("hitId：{}, {}：{}", hit.getId(), identifyField, hit.sourceAsMap().get(identifyField));
                }
            }
        }
        return docIds;
    }


    /**
     * 新建Index（database）
     *
     * @param index    索引
     * @param settings 设置shard和replicas
     */
    public void createIndex(String index, Settings.Builder settings) {
        IndicesAdminClient indicesAdminClient = esClient.admin().indices();
        settings = settings == null ? getDefaultIndexSettings() : settings;
        indicesAdminClient.prepareCreate(indexName).setSettings(settings).execute().actionGet();
    }

    /**
     * 设置Mapping（database）
     *
     * @param index          索引
     * @param type           类型
     * @param mappingJsonStr 设置field analyzer
     * @link curl -XPUT http://es1.es.com:9200/cloudx_web_v3/T_EVENT_LOG/_mapping?pretty -d
     * '{"T_EVENT_LOG":{"properties":{"name":{"type":"string","analyzer":"ik","searchAnalyzer":"ik"}}}}'
     * @link https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-admin-indices.html#java-admin-indices-put-mapping
     */
    public void setIndexMapping(String index, String type, String mappingJsonStr) {
        IndicesAdminClient indicesAdminClient = esClient.admin().indices();
        //TODO org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: mapping type is missing;
        indicesAdminClient.preparePutMapping(index, type).setSource(mappingJsonStr).get();
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
    public void createDocument_bulkProcess(String index, String type, String docJsonStr) {
        if (StringUtils.isNoneEmpty(docJsonStr)) {
            bulkProcessor.add(new IndexRequest(index, type).source(docJsonStr));
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
        log.debug("delete doc ：{}", id);
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
                .setQuery(matchAllQuery())
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

    public XContentBuilder getDefaultAnalyzerMapping(String index) {
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject(index)
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "string")
                    .field("indexAnalyzer", "ik")
                    .field("searchAnalyzer", "ik")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            String json = mapping.prettyPrint().string();
            log.debug("mapping:{}", json);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }


}
