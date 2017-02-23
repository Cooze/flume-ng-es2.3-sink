package org.apache.flume.sink.elasticsearch.client;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchIrestOptLogTransportClient implements ElasticSearchClient  {

	  public static final Logger logger = LoggerFactory
	      .getLogger(ElasticSearchIrestOptLogTransportClient.class);
	  private static final String BULK_SIZE_REGX = "^(\\d+)(\\D*)";
	  private int bulkActions;						//设置请求上限，bulk提交。例如100个请求提交一次。
	  private ByteSizeValue bulkSize = null;		//设置提交缓冲的大小，缓冲到达上限也会提交。
	  private TimeValue flushBulkTime = null;  		//设置bulk请求提交时间。
	  private int concurrentRequests;				//设置是否允许新建连接，即同步。
	  /*
	   * 设置bulk回退策略.
	   * 设置作用：当请求失败时，每隔backoffInitDelay(时间)重新请求,
	   * 		      最大允许尝试  maxNumberOfRetries 次数,当尝试失败时该请求回退。
	   */
	  private BackoffPolicy backoffPolicy=null;
	  private String bulkAwaitCloseTime;			//配置bulkProcessor等待时间
	  
	  private BulkProcessor bulkProcessor = null;	//批量处理类。
	  
	  private final Pattern pattern = Pattern.compile(BULK_SIZE_REGX,
		        Pattern.CASE_INSENSITIVE);
		    private Matcher matcher = pattern.matcher("");

	  //===========================================================//
	  private InetSocketTransportAddress[] serverAddresses;
	  private ElasticSearchEventSerializer serializer;
	  private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
	  
	  private Client client;

	  /**
	   * Transport client for external cluster
	   * 
	   * @param hostNames
	   * @param clusterName
	   * @param serializer
	   */
	  public ElasticSearchIrestOptLogTransportClient(String[] hostNames, String clusterName,
	      ElasticSearchEventSerializer serializer) {
	    configureHostnames(hostNames);
	    this.serializer = serializer;
	    openClient(clusterName);
	  }

	  public ElasticSearchIrestOptLogTransportClient(String[] hostNames, String clusterName,
	      ElasticSearchIndexRequestBuilderFactory indexBuilder) {
	    configureHostnames(hostNames);
	    this.indexRequestBuilderFactory = indexBuilder;
	    openClient(clusterName);
	  }
	  
	  
	  private void configureHostnames(String[] hostNames) {
	    logger.warn(Arrays.toString(hostNames));
	    serverAddresses = new InetSocketTransportAddress[hostNames.length];
	    for (int i = 0; i < hostNames.length; i++) {
	      String[] hostPort = hostNames[i].trim().split(":");
	      String host = hostPort[0].trim();
	      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
	              : DEFAULT_PORT;
	      serverAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
	    }
	  }
	  
	  @Override
	  public void close() {
	    if (client != null) {
	      client.close();
	    }
	    client = null;
	  }

	  @Override
	  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
	      String indexType, long ttlMs) throws Exception {
	    if(bulkProcessor==null){
	    	bulkProcessor = createBulkProcessor(client);
	    }
	    IndexRequestBuilder indexRequestBuilder = null;
	    if(indexRequestBuilderFactory==null){
	    	indexRequestBuilder = client
	  	          .prepareIndex(indexNameBuilder.getIndexName(event), indexType)
	  	          .setSource(serializer.getContentBuilder(event).bytes());
	    }else {
		      indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
			          client, indexNameBuilder.getIndexPrefix(event), indexType, event);
	    }
	    if (ttlMs > 0) {
		      indexRequestBuilder.setTTL(ttlMs);//TODO 缓存索引生存期，过期后该索引会被删除
		}
	    bulkProcessor.add(indexRequestBuilder.request());
	  }

	  @Override
	  public void execute() throws Exception {
	    try {
	      if (!awaitClose(bulkProcessor)) {
	        throw new InterruptedException("the specified waiting time elapses before all bulk requests complete");
	      }
	    } finally {
	    	bulkProcessor = createBulkProcessor(client);
	    }
	  }

	  /**
	   * Open client to elaticsearch cluster
	   * 
	   * @param clusterName
	   */
	  private void openClient(String clusterName) {
	    logger.info("Using ElasticSearch hostnames: {} ",
	        Arrays.toString(serverAddresses));
	    Settings settings = Settings.settingsBuilder()
	            .put("cluster.name", clusterName).build();
	    TransportClient transportClient = TransportClient.builder().settings(settings).build();
	    for (InetSocketTransportAddress host : serverAddresses) {
	      transportClient.addTransportAddress(host);
	    }
	    if (client != null) {
	      client.close();
	    }
	    client = transportClient;
	  }

	  @Override
	  public void configure(Context context) {
		  //TODO add configure
		  //To change body of implemented methods use File | Settings | File Templates.
		  this.bulkActions = Integer.parseInt(context.getString("bulkActions", "1000"));
		  this.bulkSize = parseBulkSize(context.getString("bulkSize", "5MB"));
		  this.flushBulkTime = parseFlushBulkTime(context.getString("flushTime", "1s"));
		  this.concurrentRequests = Integer.parseInt(context.getString("concurrentRequests", "1"));
		  this.backoffPolicy = parseBackOffPolicy(
				  context.getString("backoffInitDelay", "100ms"),
				  context.getString("maxNumberOfRetries", "3"));
		  this.bulkAwaitCloseTime = context.getString("bulkAwaitCloseTime", "100ms");
	  }
	  
	private BulkProcessor createBulkProcessor(Client client){
		  return BulkProcessor.builder(client, new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						System.out.println("beforeBulk......BulkRequest--Description-->"+request.getDescription());
					}
					@Override
					public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
						System.out.println("afterBulk......BulkResponse--Throwable-->"+failure.getMessage());
					}
					@Override
					public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
						System.out.println("afterBulk......BulkResponse--buildFailureMessage-->"+response.buildFailureMessage());
					}
		  		}).setBulkActions(bulkActions)
				  .setBulkSize(bulkSize)
				  .setFlushInterval(flushBulkTime)
				  .setConcurrentRequests(concurrentRequests)
				  .setBackoffPolicy(backoffPolicy)
				  .build();
	  }
	  
	  /**
	   * 处理bulkSize的方法
	   * @param string
	   * @return
	   */
	private ByteSizeValue parseBulkSize(String bulksize) {
		matcher = matcher.reset(bulksize);
		while (matcher.find()) {
		   if (matcher.group(2).equals("B")) {
		  	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.BYTES);
		   } else if (matcher.group(2).equals("KB")) {
		  	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.KB);
		   } else if (matcher.group(2).equals("MB")) {
		   	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.MB);
		   } else if (matcher.group(2).equals("GB")) {
		   	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.GB);
		   } else if (matcher.group(2).equals("PB")) {
		  	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.PB);
		   } else if (matcher.group(2).equals("TB")) {
		   	  return new ByteSizeValue(Long.parseLong(matcher.group(1)), ByteSizeUnit.TB);
		   } else {
		      logger.debug("Unknown bulkSize qualifier provided. Setting bulkSize to 5mb.");
		      return new ByteSizeValue(5, ByteSizeUnit.MB);
		   }
		}
		return new ByteSizeValue(5, ByteSizeUnit.MB);
	  }
	  
	private TimeValue parseFlushBulkTime(String flushTime) {
		matcher = matcher.reset(flushTime);
		while (matcher.find()) {
			if(matcher.group(2).equals("ms")){
				return TimeValue.timeValueMillis(Long.parseLong(matcher.group(1)));
			} else if(matcher.group(2).equals("s")){
				return TimeValue.timeValueSeconds(Long.parseLong(matcher.group(1)));
			}else if(matcher.group(2).equals("m")){
				return TimeValue.timeValueMinutes(Long.parseLong(matcher.group(1)));
			}else if(matcher.group(2).equals("h")){
				return TimeValue.timeValueHours(Long.parseLong(matcher.group(1)));
			}else{//default set flush bulk time is 1s.
				return TimeValue.timeValueSeconds(1);
			}
		}
		return TimeValue.timeValueSeconds(1);
	}
	
	private BackoffPolicy parseBackOffPolicy(String backoffInitDelay,String maxNumberOfRetries) {
		return BackoffPolicy.exponentialBackoff(parseFlushBulkTime(backoffInitDelay),
				 Integer.parseInt(maxNumberOfRetries));
	}

	private boolean awaitClose(BulkProcessor bulkProcessor) throws Exception{
		boolean result=false;
		matcher = matcher.reset(bulkAwaitCloseTime);
		while(matcher.find()){
			if(matcher.group(2).equals("ms")){
				result = bulkProcessor.awaitClose(Long.parseLong(matcher.group(1)), TimeUnit.MILLISECONDS);
			}else if(matcher.group(2).equals("s")){
				result = bulkProcessor.awaitClose(Long.parseLong(matcher.group(1)), TimeUnit.SECONDS);
			}else{
				result = bulkProcessor.awaitClose(100, TimeUnit.MILLISECONDS);
			}
		}
		return result;
	}
	
}
