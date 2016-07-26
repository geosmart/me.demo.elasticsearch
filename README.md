# me.demo.elasticsearch
ElasticSearch V2.3.4 示例项目（批量索引测试），基于Spring Boot框架

## Elasticsearch数据插入
[Elasticsearch教程](http://www.tutorialspoint.com/elasticsearch/index.htm)

### 单条插入
以IndicesAdminClient新建Index,Type；以IndexAPI插入document(fields);

* [IndicesAdminClient ](https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-admin-indices.html)
* [Index API](https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-docs-index.html)

### 批量插入
以Bulk API进行手动批量插入，或采用BulkProcessor 进行自动分段插入；

* [bulk-api](https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-docs-bulk.html)
* [bulk-processor](https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-docs-bulk-processor.html)
* [bulk-api-examples](http://www.programcreek.com/java-api-examples/index.php?api=org.elasticsearch.action.bulk.BulkRequestBuilder)
 
