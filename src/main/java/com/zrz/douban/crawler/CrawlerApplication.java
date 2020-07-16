package com.zrz.douban.crawler;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.Charsets;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.FileOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


public class CrawlerApplication {

    @Data
    @NoArgsConstructor
    @Builder
    @AllArgsConstructor
    static class Book {
        private String title;
        private float score;
        private int people;
        private String link;
    }

    @Data
    @NoArgsConstructor
    @Builder
    @AllArgsConstructor
    static class SearchCriteria {
        private String tagName;
        private float score;
        private int people;

    }

    // 初始化线程池
    static final Executor executor = new ThreadPoolExecutor(3, 3, 30, TimeUnit.SECONDS
            , new LinkedBlockingQueue<>(1000)
            , new ThreadFactory() {
        private final AtomicInteger threadCount = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "crawler-async-executor-" + threadCount.incrementAndGet());
        }
    }
            , new ThreadPoolExecutor.AbortPolicy()
    );

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        long startTime = System.currentTimeMillis();
        List<SearchCriteria> criteriaList = new ArrayList<>(Arrays.asList(
                new SearchCriteria("生活", 8.5f, 2000),
                new SearchCriteria("生活", 8.5f, 5000),
                new SearchCriteria("生活", 9f, 2000),
                new SearchCriteria("生活", 9f, 10000),
                new SearchCriteria("科技", 8.5f, 2000),
                new SearchCriteria("科技", 8.5f, 5000),
                new SearchCriteria("科技", 9f, 2000),
                new SearchCriteria("科技", 9f, 10000),
                new SearchCriteria("文化", 8.5f, 2000),
                new SearchCriteria("文化", 8.5f, 5000),
                new SearchCriteria("文化", 9f, 2000),
                new SearchCriteria("文化", 9f, 10000),
                new SearchCriteria("经管", 8.5f, 2000),
                new SearchCriteria("经管", 8.5f, 5000),
                new SearchCriteria("经管", 9f, 2000),
                new SearchCriteria("经管", 9f, 10000)
        ));

        criteriaList.forEach(criteria -> {
            ConcurrentHashMap<String, Book> crawlResult = new ConcurrentHashMap<>(500);
            List<String> linkList = getTagLinks(criteria.tagName);

            CompletableFuture<Void> future = null;
            for (String url : linkList) {
                future = CompletableFuture.runAsync(() -> {
                    parseBookDesc(url, criteria, crawlResult);
                }, executor);
            }

            CompletableFuture.allOf(future).join();
            String fileName = String.format("%s-%s-%s.csv", criteria.getTagName(), criteria.getScore(), criteria.getPeople());
            System.out.println(String.format("----------------开始保存数据:%s\n", fileName));
            StringBuilder stringBuilder = new StringBuilder("标题,评分,人数,链接\n");
            for (Map.Entry<String, Book> entry: crawlResult.entrySet() ) {
                stringBuilder.append(String.format("%s,%s,%s,%s\n", entry.getKey(), entry.getValue().getScore(), entry.getValue().getPeople(), entry.getValue().getLink()));
            }

            try(FileOutputStream outputStream = new FileOutputStream(fileName)) {
                outputStream.write(stringBuilder.toString().getBytes(Charsets.UTF_8));
                System.out.println(String.format("----------------数据保存成功:%s\n", fileName));
            } catch (IOException e) {
                System.out.println(String.format("----------------数据保存失败！！！:%s\n", fileName));
            }

        });

        long endTime = System.currentTimeMillis();

        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }

    private static List<String> getTagLinks(String tagName) {
        String baseURL = "https://book.douban.com";
        String startURL = baseURL + "/tag/?view=type&icn=index-sorttags-all";
        List<String> linkList = new ArrayList<>();
        Document doc = getDoc(startURL);
        doc.select("#content > div > div.article > div:nth-child(2) a[name=" + tagName + "]").next().select("a").forEach(x -> {
            linkList.add(baseURL + x.attr("href"));
        });

        return linkList;
    }

    /**
     * 通过url获取符合指定筛选条件的书籍信息
     *
     * @param url            请求地址
     * @param searchCriteria 筛选条件
     * @param crawlerResult  抓取结果
     */
    private static void parseBookDesc(String url, SearchCriteria searchCriteria, ConcurrentHashMap<String, Book> crawlerResult) {
        int pageCount = 0;
        Document doc = getDoc(url);

        try {
            pageCount = Integer.parseInt(doc.select("#subject_list > div.paginator >a").last().text());
        } catch (Exception ex) {
            System.out.println("url:" + url + ",页码转换错误:" + ex.getMessage());
            pageCount = 1;
        }

        CompletableFuture<Void> completableFuture = null;
        for (int i = 0; i < pageCount; i++) {
            final int finalI = i;
            completableFuture = CompletableFuture.runAsync(() -> {
                String pagedListUrl = String.format("%s?start%s&type=T", url, finalI * 20);
                Document pagedListDoc = getDoc(pagedListUrl);
                pagedListDoc.select("#subject_list > ul > li").forEach(ele -> {
                    String title = ele.select(".info>h2>a").text()
                            .replace("\n", "")
                            .replace(" ", "");
                    String link = ele.select(".info>h2>a").attr("href");
                    float score = 0f;
                    try {
                        score = Float.parseFloat(ele.select(".rating_nums").text());
                    } catch (Exception ignored) {
                    }

                    int peopleNum = 0;
                    try {
                        String peopleNumStr = ele.select(".pl").text();
                        peopleNumStr = peopleNumStr.replace(" ", "")
                                .replace("(", "")
                                .replace("\n", "")
                                .replace("人评价)", "");
                        peopleNum = Integer.parseInt(peopleNumStr);

                    } catch (Exception ignored) {
                    }

                    if (score >= searchCriteria.getScore() && peopleNum >= searchCriteria.getPeople()) {
                        Book book = new Book(title, score, peopleNum, link);
                        if (crawlerResult.putIfAbsent(book.title, book) == null) {
                            System.out.println(String.format("%s,%s,%s", book.getTitle(), book.getScore(), book.getPeople()));
                        }
                    }
                });
            }, executor);

        }


        CompletableFuture.allOf(completableFuture).join();

    }

    /**
     * 通过url获取Document对象
     *
     * @param url 请求地址
     * @return Document对象
     */
    private static Document getDoc(String url) {
        Map<String, String> headers = new HashMap<>();
        headers.put("user-agent", "Chrome/83.0");
        headers.put("Cookie", "gr_user_id=d655a7dd-23c4-4f0c-8fc4-cf46a20c4f4e; _vwo_uuid_v2=DC2F2BDD29FD9988E3DFF419D0AC3506C|907f50b73382849a00392ebebc78c6cb; __yadk_uid=wUcAGwlJOa9QmXqHU8jFfLVVoCOCF993; douban-fav-remind=1; douban-profile-remind=1; __utmv=30149280.204; __gads=ID=3bbb1411b00e4ee9:T=1577672497:S=ALNI_MY-4JQDN_KRLywjJpZsXqY_lSngfw; trc_cookie_storage=taboola%2520global%253Auser-id%3D5cb24f5d-0621-4171-acdd-fcbd118103e1-tuct3f732c3; bid=oHFXpaJovIk; push_noty_num=0; push_doumail_num=0; ll=\"108090\"; Hm_lvt_cfafef0aa0076ffb1a7838fd772f844d=1592824817; ct=y; __utmc=30149280; __utmc=81379588; __utmz=30149280.1594639198.51.20.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmz=81379588.1594639198.48.36.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; viewed=\"35042630_1039311_35065701_1500149_30244461_27168433_26838785_26576861_30155569_1322455\"; dbcl2=\"2041066:Jc9RF5kSFKM\"; ck=l0Dj; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03=11c3f627-9f9f-4a7a-80f2-b56d4e0a36cd; gr_cs1_11c3f627-9f9f-4a7a-80f2-b56d4e0a36cd=user_id%3A0; _pk_ref.100001.3ac3=%5B%22%22%2C%22%22%2C1594776613%2C%22https%3A%2F%2Fwww.douban.com%2F%22%5D; _pk_id.100001.3ac3=47ad0cc4d3ca921f.1565084387.64.1594776613.1594721457.; _pk_ses.100001.3ac3=*; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03_11c3f627-9f9f-4a7a-80f2-b56d4e0a36cd=true; __utma=30149280.323262343.1577196782.1594721412.1594776613.57; __utmt_douban=1; __utmb=30149280.1.10.1594776613; __utma=81379588.604583507.1577197113.1594721412.1594776613.53; __utmt=1; __utmb=81379588.1.10.1594776613; ap_v=0,6.0");
        String html = HttpClientUtil.getInstance().get(url, headers);
        return Jsoup.parse(html);
    }
}
