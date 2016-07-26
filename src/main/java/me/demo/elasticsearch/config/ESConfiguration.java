package me.demo.elasticsearch.config;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * Created by geomart on 2016/7/25.
 */
@Configuration
public class ESConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ESConfiguration.class);

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port}")
    private String port;

    @Bean
    public Client esClient() {
        try {
            Settings settings = Settings.settingsBuilder()
//                    .put("search.min", 20)
//                    .put("search.max", 40)
                    .put("cluster.name", clusterName)
                    .put("client.transport.sniff", true)
                    .build();

            return TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port)));
        } catch (Exception e) {
            log.error("ES connection error,exception is {}.", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Bean
    public BulkProcessor bulkProcessor(@Qualifier("esClient") Client client) {
        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                log.info("ES is going to execute new bulk composed of {} actions", bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    log.info("ES bulk fail,failure message is {}", bulkResponse.buildFailureMessage());
                } else {
                    log.info("ES has executed bulk composed of {} actions", bulkRequest.numberOfActions());
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.error("ES bulk error,exception is {}", throwable.getMessage());
            }
        })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(100, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .setConcurrentRequests(4)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

}
