/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Internal ElasticSearch client factory. Responsible for creating instance
 * of ElasticSearch clients.
 */
public class ElasticSearchClientFactory {
  public static final String TransportClient = "transport";
  public static final String RestClient = "rest";
  
  //添加自己的重写的客户端
  public static final String IREST_OPT_CLIENT = "irest_transport";

  /**
   *
   * @param clientType
   *    String representation of client type
   * @param hostNames
   *    Array of strings that represents hosntames with ports (hostname:port)
   * @param clusterName
   *    Elasticsearch cluster name used only by Transport Client
   * @param serializer
   *    Serializer of flume events to elasticsearch documents
   * @return
   */
  public ElasticSearchClient getClient(String clientType, String[] hostNames,
      String clusterName, ElasticSearchEventSerializer serializer,
      ElasticSearchIndexRequestBuilderFactory indexBuilder) throws NoSuchClientTypeException {
	if (clientType.equalsIgnoreCase(IREST_OPT_CLIENT) && serializer != null) {
		//使用自定义客户端
		return new ElasticSearchIrestOptLogTransportClient(hostNames, clusterName, serializer);
	} else if (clientType.equalsIgnoreCase(TransportClient) && serializer != null) {
		return new ElasticSearchTransportClient(hostNames, clusterName, serializer);
    } else if (clientType.equalsIgnoreCase(TransportClient) && indexBuilder != null) { 
    	return new ElasticSearchTransportClient(hostNames, clusterName, indexBuilder);
    } else if (clientType.equalsIgnoreCase(RestClient) && serializer != null) {
    	return new ElasticSearchRestClient(hostNames, serializer);
    }
    throw new NoSuchClientTypeException();
  }

  /**
   * Used for tests only. Creates local elasticsearch instance client.
   *
   * @param clientType Name of client to use
   * @param serializer Serializer for the event
   * @param indexBuilder Index builder factory
   *
   * @return Local elastic search instance client
   */
  public ElasticSearchClient getLocalClient(String clientType, ElasticSearchEventSerializer serializer,
          ElasticSearchIndexRequestBuilderFactory indexBuilder) throws NoSuchClientTypeException {
	if (clientType.equalsIgnoreCase(TransportClient) && serializer != null) {
      return new ElasticSearchTransportClient(serializer);
    } else if (clientType.equalsIgnoreCase(TransportClient) && indexBuilder != null)  {
      return new ElasticSearchTransportClient(indexBuilder);
    } else if (clientType.equalsIgnoreCase(RestClient)) {
    }
    throw new NoSuchClientTypeException();
  }
  
  private static TransportClient client;
  
  public static TransportClient getClient() {
	return client;
}

public static TransportClient getClient(String hostName, String clusterName)
          throws NoSuchClientTypeException {
      if (client == null) {
          // 集群模式 设置Settings
          System.out.println(">>>>>>>>>>>>>>>" + clusterName + ">>>>>" + hostName);
          Settings settings = Settings.settingsBuilder()
                  .put("cluster.name", clusterName).build();
          try {
              client = org.elasticsearch
            		  .client
            		  .transport
            		  .TransportClient
                      .builder()
                      .settings(settings)
                      .build()
                      .addTransportAddress(
                      new InetSocketTransportAddress(InetAddress
                       .getByName(hostName), 9300));
          } catch (UnknownHostException e) {
              e.printStackTrace();
          }
      }
      return client;
  }
  
  public static String generateJson(String[] fields,String text,String splitStr) {
      String[] texts=null;
      if(!StringUtils.isEmpty(text)){
          texts=text.split(splitStr);
          if(texts.length==fields.length){
              String json = "";
              try {
                  XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                          .startObject();
                  for(int i=0;i<fields.length;i++){
                      contentBuilder.field(fields[i],texts[i]);
                  }
                  json = contentBuilder.endObject().string();
              } catch (IOException e) {
                  e.printStackTrace();
              }
              return json;
          }
      }
	throw new RuntimeException();
  }
}
