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
        headers.put("Cookie", "");
        String html = HttpClientUtil.getInstance().get(url, headers);
        return Jsoup.parse(html);
    }
}
