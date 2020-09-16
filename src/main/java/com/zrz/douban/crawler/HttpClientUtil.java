package com.zrz.douban.crawler;

import org.apache.commons.codec.Charsets;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpClientUtil implements AutoCloseable {

    private volatile static HttpClientUtil httpClientUtil;
    private final CloseableHttpClient httpClient;
    private final PoolingHttpClientConnectionManager connMgr;
    private final ThreadLocal<HttpClientContext> httpContext = ThreadLocal.withInitial(HttpClientContext::create);
    private final IdleConnectionEvictThread evictThread;

    public HttpClientUtil() {

        // 自定义keep-alive策略，keep-alive使得tcp连接可以被复用。
        // 但默认的keep-alive时长为无穷大，不符合现实，所以需要改写，定义一个更短的时间
        // 如果服务器http响应头包含 "Keep-Alive:timeout=" ，则使用timeout=后面指定的值作为keep-alive的时长，否则默认60秒
        ConnectionKeepAliveStrategy strategy = (httpResponse, httpContext) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator(httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }

            return 60 * 1000;

        };

        // 自定义连接池，连接池可以连接可以使连接可以被复用。
        // 连接的复用需要满足：Keep-alive + 连接池。keep-alive使得连接能够保持存活，不被系统销毁；连接池使得连接可以被程序重复引用
        // 默认的连接池，DefaultMaxPerRoute只有5个，MaxTotal只有10个，不满足实际的生产需求
        connMgr = new PoolingHttpClientConnectionManager();

        // 最大连接数500
        connMgr.setMaxTotal(500);
        // 同一个ip:port的请求，最大连接数200
        connMgr.setDefaultMaxPerRoute(200);

        // 启动线程池空闲连接、超时连接监控线程
        evictThread = new IdleConnectionEvictThread(connMgr);
        evictThread.start();

        // 定义请求超时配置
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(5000)  // 从连接池里获取连接的超时时间
                .setConnectTimeout(2000)            // 建立TCP的超时时间
                .setSocketTimeout(10000)             // 读取数据包的超时时间
                .build();


        httpClient = HttpClients.custom()
                .setConnectionManager(connMgr)
                .setKeepAliveStrategy(strategy)
                .setDefaultRequestConfig(requestConfig)
                .build();

    }

    /**
     * 因为HttpClient是线程安全的，可以提供给多个线程复用，同时连接池的存证的目的就是为了可以复用连接，所以提供单例模式
     */
    private static class HttpClientUtilHolder {
        private static final HttpClientUtil INSTANCE = new HttpClientUtil();
    }

    public static HttpClientUtil getInstance() {
        return HttpClientUtilHolder.INSTANCE;
    }

    public PoolingHttpClientConnectionManager getConnMgr() {
        return connMgr;
    }

    public HttpContext getHttpContext() {
        return httpContext.get();
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * http get操作
     *
     * @param url     请求地址
     * @param headers 请求头
     * @return 返回响应内容
     */
    public String get(String url, Map<String, String> headers) {
        HttpGet httpGet = new HttpGet(url);
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                httpGet.setHeader(header.getKey(), header.getValue());
            }
        }

        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpGet, httpContext.get());
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity, Charsets.UTF_8);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    //上面的EntityUtils.toString会调用inputStream.close(),进而也会触发连接释放回连接池，但因为httpClient.execute可能抛异常，所以得在finally显示调一次，确保连接一定被释放
                    response.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * form表单post
     *
     * @param url     请求地址
     * @param params  参数内容
     * @param headers http头信息
     * @return http文本格式响应内容
     */
    public String postWithForm(String url, Map<String, String> params, Map<String, String> headers) {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }
        }

        List<NameValuePair> formParams = new ArrayList<>();
        if (params != null) {
            for (Map.Entry<String, String> param : params.entrySet()) {
                formParams.add(new BasicNameValuePair(param.getKey(), param.getValue()));
            }
        }

        UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(formParams, Charsets.UTF_8);
        httpPost.setEntity(formEntity);
        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpPost, httpContext.get());
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity, Charsets.UTF_8);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * json内容post
     *
     * @param url     请求地址
     * @param data    json报文
     * @param headers http头
     * @return http文本格式响应内容
     */
    public String postWithBody(String url, String data, Map<String, String> headers) {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                httpPost.setHeader(header.getKey(), header.getValue());
            }
        }

        StringEntity stringEntity = new StringEntity(data, ContentType.create("plain/text", Charsets.UTF_8));
        httpPost.setEntity(stringEntity);
        httpPost.setHeader("Content-type", "application/json");
        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpPost, httpContext.get());
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity, Charsets.UTF_8);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * HttpClientUtil类回收时，通过httpClient.close()，可以间接的关闭连接池，从而关闭连接池持有的所有tcp连接
     * 与response.close()的区别：response.close()只是把某个请求持有的tcp连接放回连接池，而httpClient().close()是销毁整个连接池
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        httpClient.close();
        evictThread.shutdown();
    }

    /**
     * 定义一个连接监控线程类，用以从连接池里关闭过期的连接（即服务器已经关闭的连接），以及在30秒内处于空闲的连接；每5秒钟处理一次
     */
    static class IdleConnectionEvictThread extends Thread {
        private final HttpClientConnectionManager connMgr;
        private volatile boolean shutdown;

        public IdleConnectionEvictThread(HttpClientConnectionManager connMgr) {
            super();
            this.connMgr = connMgr;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                synchronized (this) {
                    while (!shutdown) {
                        wait(5000);
                        connMgr.closeExpiredConnections();
                        connMgr.closeIdleConnections(30, TimeUnit.SECONDS);
                    }
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        /**
         * 关闭监控线程
         */
        public void shutdown() {
            // 监控线程可能还处于wait()状态，通过notifyAll()唤醒，及时退出while循环
            synchronized (this) {
                shutdown = true;
                notifyAll();
            }
        }
    }
}