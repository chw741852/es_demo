package com.hong.es;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.Operator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
public class BookTest {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private BookRepository bookRepository;
    @Autowired
    private RoomProductPriceRepository priceRepository;

    @Before
    public void init() {
        if (bookRepository.count() == 0) {
            List<Book> books = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                Book book = new Book();
                book.setTitle(getTitle().get(i));
                book.setContent(getContent().get(i));
                book.setWeight(i);
                book.setUserId(i % 10);
                books.add(book);
            }
            bookRepository.saveAll(books);
        }
    }

    /**
     * 模糊查询
     * 共13个结果，按照默认的排序方式，即匹配相关度排序，前10个匹配度最高，都是完全带“浣溪沙”三个字的。
     * 第10个、11个都是题目和正文都包含“溪”字而且出现了2次“溪”，最后一个是正文带一个“溪”。
     */
    @Test
    public void singleWord() {
        String word = "浣溪沙";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery(word)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 按weight排序
     */
    @Test
    public void singleWordSortByWeight() {
        String word = "浣溪沙";
        Pageable pageable = PageRequest.of(0, 20, new Sort(Sort.Direction.DESC, "weight"));
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery(word)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 只针对content字段模糊匹配
     */
    @Test
    public void singleMatch() {
        String word = "落日熔金";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchQuery("content", word)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 单字段，短语匹配
     * 一个针对短语“中华共和国”的查询不会匹配“中华人民共和国”
     * 这种完全匹配比较严格，类似于数据库里的“%落日熔金%”这种
     */
    @Test
    public void singlePhraseMatch() {
        String word = "落日熔金";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchPhraseQuery("content", word)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * matchPhraseQuery("content", word).slop(2)   // 允许跳过中间2位字符
     * 此时搜索“中华共和国”，可匹配到“中华人民共和国”
     */
    @Test
    public void singlePhraseMatch2() {
        String word = "中华共和国";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchPhraseQuery("content", word).slop(2)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * term查询
     * 这个是最严格的匹配，属于低级查询，不进行分词的
     */
    @Test
    public void singleTerm() {
        int userId = 1;
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("userId", userId)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 多个字段匹配
     */
    @Test
    public void multiMatch() {
        String title = "我是";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(multiMatchQuery(title, "title", "content")).withPageable(pageable).build();
    }

    /**
     * 完全包含查询
     * 当我们输入“我天”时，ES会把分词后所有包含“我”和“天”的都查询出来，如果我们希望必须是包含了两个字的才能被查询出来，那么我们就需要设置一下Operator
     */
    @Test
    public void contain() {
        String word = "浣溪沙";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery(word).defaultOperator(Operator.AND)).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 完全包含查询
     * 如果设置operator为and的话，譬如用户输入了5个词，但包含了4个，也是显示不出来的。我们可以通过设置精度来控制。
     */
    @Test
    public void contain2() {
        String word = "浣溪沙";
        Pageable pageable = PageRequest.of(0, 20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery(word).defaultOperator(Operator.AND).minimumShouldMatch("75%")).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * 合并查询
     * boolQuery，可以设置多个条件的查询方式。它的作用是用来组合多个Query，有四种方式来组合，must，mustnot，filter，should。
     * must代表返回的文档必须满足must子句的条件，会参与计算分值；
     * filter代表返回的文档必须满足filter子句的条件，但不会参与计算分值；
     * should代表返回的文档可能满足should子句的条件，也可能不满足，有多个should时满足任何一个就可以，通过minimum_should_match设置至少满足几个。
     * mustnot代表必须不满足子句的条件。
     */
    @Test
    public void bool() {
        int userId = 1;
        int weight = 10;
        String title = "浣溪沙";
        Pageable pageable = PageRequest.of(0, 20);

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(boolQuery().must(termQuery("userId", userId))
            .should(rangeQuery("weight").lt(weight)).must(matchQuery("title", title))).withPageable(pageable).build();
        search(searchQuery);
    }

    /**
     * Query和Filter的区别
     * 1 查询上下文：查询操作不仅仅会进行查询，还会计算分值，用于确定相关度；
     * 2 过滤器上下文：查询操作仅判断是否满足查询条件，不会计算得分，查询的结果可以被缓存。
     */

    /**
     * 搜索
     * @param searchQuery query
     */
    private void search(SearchQuery searchQuery) {
        List<Book> books = elasticsearchTemplate.queryForList(searchQuery, Book.class);
        System.out.println("数据：");
        System.out.println(JSONObject.toJSONString(books));
    }

    private List<String> getTitle() {
        List<String> list = new ArrayList<>();
        list.add("《如梦令·常记溪亭日暮》");
        list.add("《醉花阴·薄雾浓云愁永昼》");
        list.add("《声声慢·寻寻觅觅》");
        list.add("《永遇乐·落日熔金》");
        list.add("《如梦令·昨夜雨疏风骤》");
        list.add("《渔家傲·雪里已知春信至》");
        list.add("《点绛唇·蹴[1]罢秋千》");
        list.add("《点绛唇·寂寞深闺》");
        list.add("《蝶恋花·泪湿罗衣脂粉满》");
        list.add("《蝶恋花 离情》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《减字木兰花·卖花担上》");
        list.add("《临江仙·欧阳公作《蝶恋花》");
        list.add("《临江仙·庭院深深深几许》");
        list.add("《念奴娇·萧条庭院》");
        list.add("《菩萨蛮·风柔日薄春犹早》");
        list.add("《菩萨蛮·归鸿声断残云碧》");
        list.add("《武陵春·风住尘香花已尽》");
        list.add("《一剪梅·红藕香残玉蕈秋》");
        list.add("《渔家傲·天接云涛连晓雾》");
        list.add("《鹧鸪天·暗淡轻黄体性柔》");
        list.add("《鹧鸪天·寒日萧萧上锁窗》");
        list.add("《一剪梅·红藕香残玉簟秋》");
        list.add("《如梦令·常记溪亭日暮》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《蝶恋花·泪湿罗衣脂粉满》");
        list.add("《蝶恋花·暖日晴风初破冻》");
        list.add("《鹧鸪天·寒日萧萧上锁窗》");
        list.add("《醉花阴·薄雾浓云愁永昼》");
        list.add("《鹧鸪天·暗淡轻黄体性柔》");
        list.add("《蝶恋花·永夜恹恹欢意少》");
        list.add("《浣溪沙》");
        list.add("《浣溪沙》");
        list.add("《如梦令·谁伴明窗独坐》");
        return list;
    }

    private List<String> getContent() {
        List<String> list = new ArrayList<>();
        list.add("初中 宋·李清照 常记溪亭日暮，沉醉不知归路，兴尽晚回舟，误入藕花深处。争渡，争渡");
        list.add("重阳节 宋·李清照 薄雾浓云愁永昼，瑞脑消金兽。佳节又重阳，玉枕纱厨，半夜凉初透。东");
        list.add("闺怨诗 宋·李清照 寻寻觅觅，冷冷清清，凄凄惨惨戚戚。乍暖还寒时候，最难将息。三杯两");
        list.add("元宵节 宋·李清照 落日熔金，暮云合璧，人在何处。染柳烟浓，吹梅笛怨，春意知几许。元");
        list.add("婉约诗 宋·李清照 昨夜雨疏风骤，浓睡不消残酒，试问卷帘人，却道海棠依旧。知否，知否");
        list.add("描写梅花 宋·李清照 雪里已知春信至，寒梅点缀琼枝腻，香脸半开娇旖旎，当庭际，玉人浴出");
        list.add(" 宋·李清照 蹴罢秋千，起来慵整纤纤手。露浓花瘦，薄汗轻衣透。见客入来，袜刬金");
        list.add("闺怨诗 宋·李清照 寂寞深闺，柔肠一寸愁千缕。惜春春去。几点催花雨。倚遍阑干，只是无");
        list.add("婉约诗 宋·李清照 泪湿罗衣脂粉满。四叠阳关，唱到千千遍。人道山长水又断。萧萧微雨闻");
        list.add("描写春天 宋·李清照 暖雨晴风初破冻，柳眼梅腮，已觉春心动。酒意诗情谁与共？泪融残粉花");
        list.add("寒食节 宋·李清照 淡荡春光寒食天，玉炉沈水袅残烟，梦回山枕隐花钿。海燕未来人斗草，");
        list.add(" 宋·李清照 髻子伤春慵更梳，晚风庭院落梅初，淡云来往月疏疏，玉鸭薰炉闲瑞脑，");
        list.add(" 宋·李清照 莫许杯深琥珀浓，未成沉醉意先融。疏钟已应晚来风。瑞脑香消魂梦断，");
        list.add("闺怨诗 宋·李清照 小院闲窗春已深，重帘未卷影沉沉。倚楼无语理瑶琴，远岫出山催薄暮。");
        list.add("爱情诗 宋·李清照 绣幕芙蓉一笑开，斜偎宝鸭亲香腮，眼波才动被人猜。一面风情深有韵，");
        list.add("描写春天 宋·李清照 卖花担上，买得一枝春欲放。泪染轻匀，犹带彤霞晓露痕。怕郎猜道，奴");
        list.add("》 宋·李清照 欧阳公作《蝶恋花》，有“深深深几许”之句，予酷爱之。用其语作“庭");
        list.add("描写梅花 宋·李清照 庭院深深深几许，云窗雾阁春迟，为谁憔悴损芳姿。夜来清梦好，应是发");
        list.add("寒食节 宋·李清照 萧条庭院，又斜风细雨，重门须闭。宠柳娇花寒食近，种种恼人天气。险");
        list.add("思乡诗 宋·李清照 风柔日薄春犹早，夹衫乍著心情好。睡起觉微寒，梅花鬓上残。故乡何处");
        list.add("描写春天 宋·李清照 归鸿声断残云碧，背窗雪落炉烟直。烛底凤钗明，钗头人胜轻。角声催晓");
        list.add("闺怨诗 宋·李清照 风住尘香花已尽，日晚倦梳头。物是人非事事休，欲语泪先流。闻说双溪");
        list.add(" 宋·李清照 红藕香残玉蕈秋，轻解罗裳，独上兰舟。云中谁寄锦书来？雁字回时，月");
        list.add("豪放诗 宋·李清照 天接云涛连晓雾。星河欲转千帆舞。仿佛梦魂归帝所。闻天语。殷勤问我");
        list.add("描写花 宋·李清照 暗淡轻黄体性柔。情疏迹远只香留。何须浅碧深红色，自是花中第一流。");
        list.add("描写秋天 宋·李清照 寒日萧萧上琐窗，梧桐应恨夜来霜。酒阑更喜团茶苦，梦断偏宜瑞脑香。");
        list.add("闺怨诗 宋·李清照 红藕香残玉簟秋。轻解罗裳，独上兰舟。云中谁寄锦书来？雁字回时，月");
        list.add(" 宋·李清照 常记溪亭日暮。沈醉不知归路。兴尽晚回舟，误入藕花深处。争渡。争渡");
        list.add(" 宋·李清照 莫许杯深琥珀浓。未成沈醉意先融。已应晚来风。瑞脑香消魂梦断，");
        list.add(" 宋·李清照 小院闲窗春色深。重帘未卷影沈沈。倚楼无语理瑶琴。远岫出山催薄暮，");
        list.add(" 宋·李清照 淡荡春光寒食天。玉炉沈水袅残烟。梦回山枕隐花钿。海燕未来人斗草，");
        list.add(" 宋·李清照 泪湿罗衣脂粉满。四叠阳关，唱到千千遍。人道山长山又断。萧萧微雨闻");
        list.add(" 宋·李清照 暖日晴风初破冻。柳眼眉腮，已觉春心动。酒意诗情谁与共。泪融残粉花");
        list.add(" 宋·李清照 寒日萧萧上锁窗。梧桐应恨夜来霜。酒阑更喜团茶苦，梦断偏宜瑞脑香。");
        list.add(" 宋·李清照 薄雾浓云愁永昼。瑞脑消金兽。佳节又重阳，玉枕纱厨，半夜凉初透。东");
        list.add(" 宋·李清照 暗淡轻黄体性柔。情疏迹远只香留。何须浅碧深红色，自是花中第一流。");
        list.add(" 宋·李清照 永夜恹恹欢意少。空梦长安，认取长安道。为报今年春色好。花光月影宜");
        list.add(" 宋·李清照 髻子伤春慵更梳。晚风庭院落梅初。淡云来往月疏疏。玉鸭熏炉闲瑞脑，");
        list.add(" 宋·李清照 绣面芙蓉一笑开。斜飞宝鸭衬香腮。眼波才动被人猜。一面风情深有韵，");
        list.add(" 宋·李清照 谁伴明窗独坐，我共影儿俩个。灯尽欲眠时，影也把人抛躲。无那，无那");
        return list;
    }
}