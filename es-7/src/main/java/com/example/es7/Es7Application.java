package com.example.es7;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.List;
import java.util.Map;

//@SpringBootApplication
public class Es7Application {
    //????????????
    static void createIndexResponse(RestHighLevelClient esClient) throws IOException {
        //????????????
        CreateIndexRequest request = new CreateIndexRequest("user2");
        CreateIndexResponse createIndexResponse = esClient.indices().create(request, RequestOptions.DEFAULT);
        //????????????
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("acknowledged: " + acknowledged);

    }
    //????????????
    static void queryIndexResponse(RestHighLevelClient esClient) throws IOException {
        GetIndexRequest user = new GetIndexRequest("user");
        GetIndexResponse getIndexResponse = esClient.indices().get(user, RequestOptions.DEFAULT);
        Map<String, List<AliasMetadata>> aliases = getIndexResponse.getAliases();
        System.out.println("aliases: " + aliases);
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
    }
    //????????????
    static void deleteIndexResponse(RestHighLevelClient esClient) throws IOException {
        DeleteIndexRequest user = new DeleteIndexRequest("user1");
        AcknowledgedResponse delete = esClient.indices().delete(user, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    //????????????
    static void insertDoc(RestHighLevelClient esClient) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("user").id("1001");
        User user = new User();
        user.setName("zhangsan");
        user.setAge(30);
        user.setSex("???");
        String userJson=JSON.toJSONString(user);
        indexRequest.source(userJson, XContentType.JSON);
        IndexResponse response = esClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }
    //????????????
    static void updateDoc(RestHighLevelClient esClient) throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON,"sex","???");
        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }
    //????????????
    static void getDoc(RestHighLevelClient esClient) throws IOException {
        GetRequest request = new GetRequest();
        request.index("user").id("1001");
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
    }
    //????????????
    static void deleteDoc(RestHighLevelClient esClient) throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }
    //????????????
    static void insertBatchDoc(RestHighLevelClient esClient) throws IOException {
        BulkRequest request = new BulkRequest();
        request.add( new IndexRequest().index("user").id("1003").source(XContentType.JSON,"name","zhangsan","sex","???","age",30));
        request.add( new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","lisi","sex","???","age",30));
        request.add( new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","wangwu","sex","???","age",40));
        request.add( new IndexRequest().index("user").id("1005").source(XContentType.JSON,"name","zhaolv","sex","???","age",50));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        //??????
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }
    //????????????
    static void deleteBatchDoc(RestHighLevelClient esClient) throws IOException {
        BulkRequest request = new BulkRequest();
        request.add( new DeleteRequest().index("user").id("1003"));
        request.add( new DeleteRequest().index("user").id("1004"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        //??????
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }
    //??????????????????
    static void queryDoc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query1Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age",30)));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query2Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //???pageNum-1???*pagesize
        builder.from(0);
        builder.size(2);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query3Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //???pageNum-1???*pagesize
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query4Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] excludes={};
        String[] includes={"name"};
        builder.fetchSource(includes,excludes);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("?????????????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query5Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //and
//        boolQueryBuilder.must(QueryBuilders.matchQuery("age",40));
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex","???"));
        //or
        boolQueryBuilder.should(QueryBuilders.matchQuery("age",30));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age",40));

        builder.query(boolQueryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query6Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        //>=
        rangeQuery.gte(30);
        //<=
        rangeQuery.lte(40);

        builder.query(rangeQuery);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query7Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("name", "zhangsan").fuzziness(Fuzziness.ONE);

        builder.query(queryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query8Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "zhangsan");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");
        builder.highlighter(highlightBuilder);
        builder.query(termsQueryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query9Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MaxAggregationBuilder field = AggregationBuilders.max("maxAge").field("age");
        builder.aggregation(field);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    //????????????
    static void query10Doc(RestHighLevelClient esClient) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("Groupage").field("age");
        builder.aggregation(aggregationBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println("???????????????"+hit.getSourceAsString());
        }
    }
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        query10Doc(esClient);
        //createIndexResponse(esClient);
        //queryIndexResponse(esClient);
        //deleteIndexResponse(esClient);
        //insertDoc(esClient);
        //updateDoc(esClient);
        //getDoc(esClient);
        //deleteDoc(esClient);
        //insertBatchDoc(esClient);
        //deleteBatchDoc(esClient);
        esClient.close();
    }
}
