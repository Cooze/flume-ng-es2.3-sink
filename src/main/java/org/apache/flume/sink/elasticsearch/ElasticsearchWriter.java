package org.apache.flume.sink.elasticsearch;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.sink.elasticsearch.client.ElasticSearchClientFactory;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

public class ElasticsearchWriter {
	private static Logger log = Logger.getLogger(ElasticsearchWriter.class);
    private static int maxCacheCount = 1000; // 缓存大小，当达到该上限时提交
    private static Vector<IndexRequestBuilder> cache = null; // 缓存
    public static Lock commitLock = new ReentrantLock(); // 在添加缓存或进行提交时加锁
    private static int maxCommitTime = 2; // 最大提交时间，s
    public static Client client;

    static {
        log.info("elasticsearch init param");
        try {
            client=ElasticSearchClientFactory.getClient();
            cache = new Vector<IndexRequestBuilder>(maxCacheCount);
            // 启动定时任务，第一次延迟10执行,之后每隔指定时间执行一次
            Timer timer = new Timer();
            timer.schedule(new CommitTimer(), 10 * 1000, maxCommitTime * 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    // 批量索引数据
    public static boolean blukIndex(List<IndexRequestBuilder> indexRequestBuilders) {
        Boolean isSucceed = true;
        BulkRequestBuilder bulkBuilder = client.prepareBulk().setRefresh(true);
        for (IndexRequestBuilder indexRequestBuilder : indexRequestBuilders) {
            bulkBuilder.add(indexRequestBuilder);
        }
        BulkResponse reponse = bulkBuilder.execute().actionGet();
        if (reponse.hasFailures()) {
            isSucceed = false;
         }
        return isSucceed;
    }
    /**
     * 添加记录到cache，如果cache达到maxCacheCount，则提交
     */
    public static void addDocToCache(IndexRequestBuilder irb,int bachSize) {
        maxCacheCount=bachSize;
        commitLock.lock();
        try {
            cache.add(irb);
            log.info("cache commit maxCacheCount:"+maxCacheCount);
            if (cache.size() >= maxCacheCount) {
                log.info("cache commit count:"+cache.size());
                blukIndex(cache);
                cache.clear();
            }
        } catch (Exception ex) {
            log.info(ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }
    /**
     * 提交定时器
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                if (cache.size() > 0) { //大于0则提交
                    log.info("timer commit count:"+cache.size());
                    blukIndex(cache);
                    cache.clear();
                }
            } catch (Exception ex) {
                log.info(ex.getMessage());
            } finally {
                commitLock.unlock();
            }
        }
    }

}
