package org.apache.flume.sink.elasticsearch;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClientFactory;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

public class ElasticsearchSinkDefine  extends AbstractSink implements Configurable{
	private static Logger log = Logger.getLogger(ElasticsearchSinkDefine.class);
    private String clusterName;    //集群名称
    private String indexName;  //索引名称
    private String indexType;  //文档名称
    private String hostName;   //主机IP
    private long ttlMs=-1;    //Elasticsearch的字段TTL失效时间
    private String[] fields;   //字段
    private String splitStr;   //扫描文本文件字段分隔符
    private int batchSize;    //缓冲提交数

    private final Pattern pattern = Pattern.compile("^(\\d+)(\\D*)",Pattern.CASE_INSENSITIVE);
    private Matcher matcher = pattern.matcher("");
    private Client client=null;    //Elasticsearch客户端

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            String out = new String(event.getBody(),"UTF-8");
            // Send the Event to the external repository.
            log.info("文本行信息>>>>"+out);
            int sz=out.split(splitStr).length;
            if(sz==fields.length){
                String json=ElasticSearchClientFactory.generateJson(fields,out,splitStr);
                System.out.println(json);
                IndexRequestBuilder irb=client.prepareIndex(indexName, indexType).setSource(json);
                irb.setTTL(ttlMs);
                IndexResponse in= irb.get();
                System.out.println();
                System.out.println("写入ES："+in.isCreated());
                //存入缓冲,这个有问题
//                ElasticsearchWriter.addDocToCache(irb,batchSize);
            }
            
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        String cn=context.getString("clusterName", "ClusterName");
        String in=context.getString("indexName", "IndexName");
        String it=context.getString("indexType", "IndexType");
        String hn=context.getString("hostName", "127.0.0.1");
        String tt=context.getString("ttl", "7*24*60*60*1000");
        String fs=context.getString("fields", "content");
        String ss=context.getString("splitStr", "\\|");
        String bs=context.getString("batchSize","10");
        this.clusterName=cn;
        this.indexName=in;
        this.indexType=it;
        this.hostName=hn;
        this.ttlMs=parseTTL(tt);
        this.splitStr=ss;
        this.fields=fs.trim().split(",");
        this.batchSize=Integer.parseInt(bs);
        for(String f:fields){
            System.out.println("field@"+f);
        }
        if(client==null){
            try {
                this.client=ElasticSearchClientFactory.getClient(hostName,clusterName);
            } catch (Exception e) {
                log.info("配置文件中：集群名字与主机有误，请检查！");
            }
        }
    }

    private long parseTTL(String ttl) {
        matcher = matcher.reset(ttl);
        while (matcher.find()) {
            if (matcher.group(2).equals("ms")) {
                return Long.parseLong(matcher.group(1));
            } else if (matcher.group(2).equals("s")) {
                return TimeUnit.SECONDS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("m")) {
                return TimeUnit.MINUTES.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("h")) {
                return TimeUnit.HOURS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("d")) {
                return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("w")) {
                return TimeUnit.DAYS.toMillis(7 * Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("")) {
                log.info("TTL qualifier is empty. Defaulting to day qualifier.");
                return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
            } else {
                log.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
                return 0;
            }
        }
        log.info("TTL not provided. Skipping the TTL config by returning 0.");
        return 0;
    }

}
