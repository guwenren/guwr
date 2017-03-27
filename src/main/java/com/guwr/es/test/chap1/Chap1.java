package com.guwr.es.test.chap1;


import com.alibaba.fastjson.JSON;
import com.guwr.es.test.model.Blog;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by   guwr
 * Project_name es-test
 * Path         com.guwr.es.test.chap1.Chap1
 * Date         2017/3/27
 * Time         10:08
 * Description
 */
public class Chap1 {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

        try {
            TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            System.out.println("client = " + client);
//            initData(client);
            matchAllQuery(client);

//            update(client);
//            delete(client);
//            deleteIndex(client);
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private static void matchAllQuery(TransportClient client) {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article").setQuery(matchAllQueryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        for (SearchHit hit : hits) {
            System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());// .get("title")
        }

    }

    private static void multiMatchQuery(TransportClient client) {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("git", "title", "content");
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article").setQuery(multiMatchQueryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        for (SearchHit hit : hits) {
            System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());// .get("title")
        }
    }

    private static void termQuery(TransportClient client) {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "hibernate");
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article").setQuery(termQueryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        for (SearchHit hit : hits) {
            System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());// .get("title")
        }
    }

    private static void update(TransportClient client) throws ExecutionException, InterruptedException, IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("AVsNwfoqv6pQR3v_Xun0");
        updateRequest.doc(jsonBuilder().startObject().field("content", "学习目标 掌握java泛型的产生意义").endObject());
        client.update(updateRequest).get();
    }

    private static void delete(TransportClient client) throws ExecutionException, InterruptedException {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("blog");
        deleteRequest.type("article");
        deleteRequest.id("AVsNwfjiv6pQR3v_Xunz");
        client.delete(deleteRequest).get();
    }

    private static void deleteIndex(TransportClient client) {
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest("blog");
        client.admin().indices().exists(indicesExistsRequest).actionGet();
        client.admin().indices().prepareDelete("blog")
                .execute().actionGet();
    }

    private static void initData(TransportClient client) {
        String data1 = JSON.toJSONString(new Blog(1, "git简介", "2016-06-19", "SVN与Git最主要的区别..."));
        String data2 = JSON.toJSONString(new Blog(2, "Java中泛型的介绍与简单使用", "2016-06-19", "学习目标 掌握泛型的产生意义..."));
        String data3 = JSON.toJSONString(new Blog(3, "SQL基本操作", "2016-06-19", "基本操作：CRUD ..."));
        String data4 = JSON.toJSONString(new Blog(4, "Hibernate框架基础", "2016-06-19", "Hibernate框架基础..."));
        String data5 = JSON.toJSONString(new Blog(5, "Git基本知识git", "2016-06-19", "Shell是什么..."));
        String data6 = JSON.toJSONString(new Blog(6, "C++基本知识", "2016-06-19", "Shell是什么..."));
        String data7 = JSON.toJSONString(new Blog(7, "Mysql基本知识", "2016-06-19", "git是什么..."));

        client.prepareIndex("blog", "article").setId("1").setSource(data1).get();
        client.prepareIndex("blog", "article").setId("2").setSource(data2).get();
        client.prepareIndex("blog", "article").setId("3").setSource(data3).get();
        client.prepareIndex("blog", "article").setId("4").setSource(data4).get();
        client.prepareIndex("blog", "article").setId("5").setSource(data5).get();
        client.prepareIndex("blog", "article").setId("6").setSource(data6).get();
        client.prepareIndex("blog", "article").setId("7").setSource(data7).get();
    }
}
