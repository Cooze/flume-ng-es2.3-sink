package org.apache.flume.sink.elasticsearch;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ElasticsearchIrestOptEvenSerializer implements ElasticSearchEventSerializer {
	
	private String logFieldToES;				//读取日志字段，admin,level
	private String[] fields;					//保存存入ES中的字段
	private String contentSplit;				//切割文本的标示符
	
	//读取解析规则 事件规则，处理文本事件。
	@Override
	public void configure(Context context) {
		contentSplit = context.getString("contentSplit", "\\|");	//读取文本切割符，默认使用  "|"
		logFieldToES = context.getString("logFieldToES");		//读取字段
		this.fields = StringUtils.isEmpty(logFieldToES)?null:logFieldToES.split(",");//使用","来分割字段
	}
	@Override
	public void configure(ComponentConfiguration conf) {
		
	}
	
	//文本事件处理
	@Override
	public XContentBuilder getContentBuilder(Event event) throws IOException {
		return appendBody(fields, new String(event.getBody(),"UTF-8"), contentSplit);
	}
	//追加文本
	public XContentBuilder appendBody(String[] fields,String text,String splitStr) {
	      String[] texts=null;
	      if(!StringUtils.isEmpty(text)){
	          texts=text.split(splitStr);
	          if(texts.length==fields.length){
	              try {
	            	  XContentBuilder builder = jsonBuilder().startObject();
	                  for(int i=0;i<fields.length;i++){
	                	  builder.field(fields[i], new String(
	                			  StringUtils.isEmpty(texts[i])
	                			  ?"".getBytes():texts[i].getBytes(), charset));
	                  }
	                  builder.endObject();
	                  return builder;
	              } catch (IOException e) {
	                  e.printStackTrace();
	              }
	          }
	      }
	      throw new RuntimeException("创建Json对象错误");
	  }
}
