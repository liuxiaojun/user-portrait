package com.jdd.datacenter.user.bolts;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
/**
 * news notify
 * @author hadoop
 *
 */
public class NewsNotifierBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(NewsNotifierBolt.class);
	private String webserver;
	private HttpClient client;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.webserver = (String)stormConf.get("webserver");
		reconnect();
	}
	
	private void reconnect(){
		client = new DefaultHttpClient();
	}

	
	@Override
	public void execute(Tuple input) {
		String product = input.getString(0);
		String categ = input.getString(1);
		int visits = input.getInteger(2);
		
		String content = "{\"product\":\""+product+"\",\"categ\":\""+categ+"\",\"visits\":" + visits+"}";
		try {
			HttpPost post = new HttpPost(webserver);	
			post.setEntity(new StringEntity(content));	
			HttpResponse response = client.execute(post);
			org.apache.http.util.EntityUtils.consume(response.getEntity());
		} catch (Exception e) {
			LOGGER.error(" NewsNotifierBolt execute throw exception",e);
			reconnect();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
