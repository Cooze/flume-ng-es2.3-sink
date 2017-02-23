package org.apache.flume.sink.elasticsearch;


import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLIENT_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLIENT_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CLIENT_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_NAME_BUILDER_CLASS;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_SERIALIZER_CLASS;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_TTL;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME_BUILDER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME_BUILDER_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL_REGEX;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class ElasticsearchIrestOptLogSink extends AbstractSink implements Configurable{
	
	private static final Logger logger = LoggerFactory
		      .getLogger(ElasticsearchIrestOptLogSink.class);
	private static final int defaultBatchSize = 100;
	
    private int batchSize = defaultBatchSize;
    private long ttlMs = DEFAULT_TTL;							//Elasticsearch的索引失效时间，即索引生存时间
    private String clusterName = DEFAULT_CLUSTER_NAME;			//集群名称
    private String indexName = DEFAULT_INDEX_NAME;				//文档名称
    private String indexType = DEFAULT_INDEX_TYPE;				//type
    private String clientType = DEFAULT_CLIENT_TYPE;			//客户端连接方式
    private final Pattern pattern = Pattern.compile(TTL_REGEX,
        Pattern.CASE_INSENSITIVE);
    private Matcher matcher = pattern.matcher("");
    
    private String[] serverAddresses = null;

    private ElasticSearchClient client = null;
    private Context elasticSearchClientContext = null;

    private ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
    private ElasticSearchEventSerializer eventSerializer;
    private IndexNameBuilder indexNameBuilder;
    private SinkCounter sinkCounter;
    private final CounterGroup counterGroup = new CounterGroup();
    
    //处理事件
	@Override
	public Status process() throws EventDeliveryException {
	    logger.debug("processing...");
	    Status status = Status.READY;
	    Channel channel = getChannel();
	    Transaction txn = channel.getTransaction();
	    try {
	      txn.begin();
	      int count;
	      for (count = 0; count < batchSize; ++count) {
	        Event event = channel.take();

	        if (event == null) {
	          break;
	        }
	        String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());
	        client.addEvent(event, indexNameBuilder, realIndexType, ttlMs);
	      }

	      if (count <= 0) {
	        sinkCounter.incrementBatchEmptyCount();
	        counterGroup.incrementAndGet("channel.underflow");
	        status = Status.BACKOFF;
	      } else {
	        if (count < batchSize) {
	          sinkCounter.incrementBatchUnderflowCount();
	          status = Status.BACKOFF;
	        } else {
	          sinkCounter.incrementBatchCompleteCount();
	        }
	        sinkCounter.addToEventDrainAttemptCount(count);
	        client.execute();
	      }
	      txn.commit();
	      sinkCounter.addToEventDrainSuccessCount(count);
	      counterGroup.incrementAndGet("transaction.success");
	    } catch (Throwable ex) {
	      try {
	        txn.rollback();
	        counterGroup.incrementAndGet("transaction.rollback");
	      } catch (Exception ex2) {
	        logger.error(
	            "Exception in rollback. Rollback might not have been successful.",
	            ex2);
	      }

	      if (ex instanceof Error || ex instanceof RuntimeException) {
	        logger.error("Failed to commit transaction. Transaction rolled back.",
	            ex);
	        Throwables.propagate(ex);
	      } else {
	        logger.error("Failed to commit transaction. Transaction rolled back.",
	            ex);
	        throw new EventDeliveryException(
	            "Failed to commit transaction. Transaction rolled back.", ex);
	      }
	    } finally {
	      txn.close();
	    }
	    return status;
	  }
	
	/**
	 *配置ES sink
	 */
	@Override
	public void configure(Context context) {
	  //读取配置文件中的IP地址，例如：192.168.1.1:9300,192.168.1.2:9300
      if (StringUtils.isNotBlank(context.getString(HOSTNAMES))) {
        serverAddresses = StringUtils.deleteWhitespace(
        context.getString(HOSTNAMES)).split(",");
      }
      //确保不为空，为空则抛出异常。
      //值得借鉴
      Preconditions.checkState(serverAddresses != null
            && serverAddresses.length > 0, "Missing Param:" + HOSTNAMES);
      
      if (StringUtils.isNotBlank(context.getString(INDEX_NAME))) {
        this.indexName = context.getString(INDEX_NAME);
      }

      if (StringUtils.isNotBlank(context.getString(INDEX_TYPE))) {
        this.indexType = context.getString(INDEX_TYPE);
      }
      if (StringUtils.isNotBlank(context.getString(CLUSTER_NAME))) {
    	  this.clusterName = context.getString(CLUSTER_NAME);
	  }

	  if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
	     this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
	  }

	  if (StringUtils.isNotBlank(context.getString(TTL))) {
		  this.ttlMs = parseTTL(context.getString(TTL));
	      Preconditions.checkState(ttlMs > 0, TTL
	      + " must be greater than 0 or not set.");
	  }
	  //请求ES客户端类型：transport 为使用连接方式
	  if (StringUtils.isNotBlank(context.getString(CLIENT_TYPE))) {
	     clientType = context.getString(CLIENT_TYPE);
	  }
	  
	  //client.  提取 客户端前缀   
	  elasticSearchClientContext = new Context();
	  //The client prefix to extract the configuration that will be passed to elasticsearch client.
	  elasticSearchClientContext.putAll(context.getSubProperties(CLIENT_PREFIX));
	  
	  //序列化方式，就是将读取到的文本信息，序列化成与ES对应字段做存储。
	  //org.apache.flume.sink.elasticsearch.ElasticSearchLogStashEventSerializer
	  //serializer = ...
	  String serializerClazz = DEFAULT_SERIALIZER_CLASS;
	  if (StringUtils.isNotBlank(context.getString(SERIALIZER))) {
	     serializerClazz = context.getString(SERIALIZER);
	  }
	 //serializer.    序列化前缀
     Context serializerContext = new Context();//TODO 此对象是干嘛？
     serializerContext.putAll(context.getSubProperties(SERIALIZER_PREFIX));

     try {
    	 //通过反射实例化序列化类
    	 @SuppressWarnings("unchecked")
        Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
            .forName(serializerClazz);
        Configurable serializer = clazz.newInstance();
     //TODO 实现自己的文本处理工厂或者事件
     if (serializer instanceof ElasticSearchIndexRequestBuilderFactory) {
          indexRequestFactory
              = (ElasticSearchIndexRequestBuilderFactory) serializer;
          indexRequestFactory.configure(serializerContext);
          //实现了自己的文本处理事件
     } else if (serializer instanceof ElasticSearchEventSerializer) {
          eventSerializer = (ElasticSearchEventSerializer) serializer;
          //TODO 查看源码实现 
          eventSerializer.configure(serializerContext);
          //可配置
     } else {
          throw new IllegalArgumentException(serializerClazz
              + " is not an ElasticSearchEventSerializer");
        }
     } catch (Exception e) {
        logger.error("Could not instantiate event serializer.", e);
        Throwables.propagate(e);
    }
     
     if (sinkCounter == null) {
       sinkCounter = new SinkCounter(getName());
     }
     //获取索引名构建类
     //TODO 待处理org.apache.flume.sink.elasticsearch.TimeBasedIndexNameBuilder
      String indexNameBuilderClass = DEFAULT_INDEX_NAME_BUILDER_CLASS;
      if (StringUtils.isNotBlank(context.getString(INDEX_NAME_BUILDER))) {
        indexNameBuilderClass = context.getString(INDEX_NAME_BUILDER);
      }
      
      Context indexnameBuilderContext = new Context();
      serializerContext.putAll(
             context.getSubProperties(INDEX_NAME_BUILDER_PREFIX));
      try {
    	 //TODO 创建自己的索引名类
    	  
    	//通过反射实例化索引名对象
    	  /*
			The name to index the document to, defaults to 'flume'
			The current date in the format 'yyyy-MM-dd' will be appended to this name, 
			for example 'foo' will result in a daily index of 'foo-yyyy-MM-dd'
    	   */
        @SuppressWarnings("unchecked")
        Class<? extends IndexNameBuilder> clazz
                = (Class<? extends IndexNameBuilder>) Class
                .forName(indexNameBuilderClass);
        indexNameBuilder = clazz.newInstance();
        indexnameBuilderContext.put(INDEX_NAME, indexName);
        indexNameBuilder.configure(indexnameBuilderContext);
      } catch (Exception e) {
        logger.error("Could not instantiate index name builder.", e);
        Throwables.propagate(e);
      }

      if (sinkCounter == null) {
        sinkCounter = new SinkCounter(getName());
      }
	  Preconditions.checkState(StringUtils.isNotBlank(indexName),
          "Missing Param:" + INDEX_NAME);
      Preconditions.checkState(StringUtils.isNotBlank(indexType),
          "Missing Param:" + INDEX_TYPE);
      Preconditions.checkState(StringUtils.isNotBlank(clusterName),
          "Missing Param:" + CLUSTER_NAME);
      Preconditions.checkState(batchSize >= 1, BATCH_SIZE
         + " must be greater than 0");
    }
	//初始化时间,
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
		        logger.info("TTL qualifier is empty. Defaulting to day qualifier.");
		        return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
		      } else {
		        logger.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
		        return 0;
		      }
		    }
		    logger.info("TTL not provided. Skipping the TTL config by returning 0.");
		    return 0;
	 }
	  
	  @Override
	  public void start() {
		//创建ES客户端实例
		//TODO  实现自己的 ES client工厂类
	    ElasticSearchClientFactory clientFactory = new ElasticSearchClientFactory();

	    logger.info("ElasticSearch sink {} started");
	    sinkCounter.start();
	    try {
	      client = clientFactory.getClient(clientType, serverAddresses,
		          clusterName, eventSerializer, indexRequestFactory);
		  client.configure(elasticSearchClientContext);
	      sinkCounter.incrementConnectionCreatedCount();
	    } catch (Exception ex) {
	      ex.printStackTrace();
	      sinkCounter.incrementConnectionFailedCount();
	      if (client != null) {
	        client.close();
	        sinkCounter.incrementConnectionClosedCount();
	      }
	    }
	    super.start();
	  }
	  
	  //关闭客户端实例。
	  @Override
	  public void stop() {
	    logger.info("ElasticSearch sink {} stopping");
	    if (client != null) {
	      client.close();
	    }
	    sinkCounter.incrementConnectionClosedCount();
	    sinkCounter.stop();
	    super.stop();
	  }
}
