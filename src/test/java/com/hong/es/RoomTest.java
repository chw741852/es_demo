package com.hong.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ScrolledPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
public class RoomTest {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void findByStartDate() {
        String s = "2018-09-10";
        String e = "2018-09-12";
        TermsAggregationBuilder ters = AggregationBuilders.terms("g_productId").field("productid");
        SearchQuery query = new NativeSearchQueryBuilder().withQuery(boolQuery().must(rangeQuery("startdate").gte(s).lt(e))
                .must(rangeQuery("stock").gt(0))).addAggregation(ters)
                .withPageable(PageRequest.of(0, 10)).build();

        Aggregations aggregations = elasticsearchTemplate.query(query, SearchResponse::getAggregations);
        ScrolledPage<RoomProductPrice> page = (ScrolledPage<RoomProductPrice>)elasticsearchTemplate.startScroll(5000L, query, RoomProductPrice.class);

        Map<String, Aggregation> aggregationMap = aggregations.asMap();
        LongTerms terms = (LongTerms) aggregationMap.get("g_productId");
        List<LongTerms.Bucket> buckets = terms.getBuckets();
        StringBuilder sb = new StringBuilder();
        for (LongTerms.Bucket bucket : buckets) {
            sb.append(bucket.getKeyAsNumber()).append(",");
        }
        System.out.println(sb);

        List<RoomProductPrice> list = new ArrayList<>();
        if (page.hasContent()) {
            list.addAll(page.getContent());
            page = (ScrolledPage<RoomProductPrice>)elasticsearchTemplate.continueScroll(page.getScrollId(), 5000L, RoomProductPrice.class);
        }
        StringBuilder sb2 = new StringBuilder();
        for (RoomProductPrice roomProductPrice : list) {
            sb2.append(roomProductPrice.getProductid()).append(",");
        }
        System.out.println(sb2);
        System.out.println("总量: " + list.size());
    }

    /**
     * SELECT
     *     userId,
     *     AVG(amount) avgAmount,
     *     count(*) orderCount
     * FROM type_order
     * GROUP by userId
     * HAVING avgAmount >= 100 and orderCount >=2
     */
    @Test
    public void findByStartDateHaving() {
        String termsAlias = "titleGroup";
        TermsAggregationBuilder terms = AggregationBuilders.terms(termsAlias).field("productid");
        // 声明BucketPath，用于后面的bucket筛选
        Map<String, String> bucketPathMap = new HashMap<>();
        bucketPathMap.put("pcount", "_count");
        Script script = new Script("params.pcount > 2");

        // 构建选择器
        BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("having", bucketPathMap, script);

        terms.subAggregation(bs);
        String s = "2018-09-10";
        String e = "2018-09-12";
        SearchQuery query = new NativeSearchQueryBuilder().withIndices("t_room_product_price").withTypes("doc").withQuery(boolQuery().must(rangeQuery("startdate").from(s).to(e))
                .must(rangeQuery("stock").gt(0))).addAggregation(terms)
                .build();
//        SearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).addAggregation(terms).withIndices("tujia_test").withTypes("book").build();
        Aggregations aggregations = elasticsearchTemplate.query(query, SearchResponse::getAggregations);

        LongTerms stringTerms = aggregations.get(termsAlias);
        List<LongTerms.Bucket> buckets = stringTerms.getBuckets();
        for (LongTerms.Bucket bucket : buckets) {
            System.out.println("-------------------------");
            System.out.println(bucket.getKey());
            System.out.println("count = " + bucket.getDocCount());
            System.out.println("-------------------------");
        }
    }

    @Test
    public void findByStartDate2() {
        String s = "2018-09-10";
        String e = "2018-09-12";
        SearchQuery query = new NativeSearchQueryBuilder().withIndices("t_room_product_price").withTypes("doc")
                .withQuery(boolQuery().must(termQuery("productid", 363420)).must(rangeQuery("startdate").from(s).to(e))
                .must(rangeQuery("stock").gt(0)))
                .build();

        ScrolledPage<RoomProductPrice> page = (ScrolledPage<RoomProductPrice>)elasticsearchTemplate.startScroll(5000L, query, RoomProductPrice.class);
        List<RoomProductPrice> list = new ArrayList<>();
        while (page.hasContent()) {
            list.addAll(page.getContent());
            page = (ScrolledPage<RoomProductPrice>)elasticsearchTemplate.continueScroll(page.getScrollId(), 5000L, RoomProductPrice.class);
        }

        for (RoomProductPrice l : list) {
            System.out.println(l.getId() + "," + l.getProductid() + "," + "," + l.getStartdate() + "," + l.getStock());
        }
    }
}
