package com.jdd.datacenter.user.spout;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.jdd.datacenter.user.model.NavigationEntry;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

/**
 * user navigation 
 * @author hadoop
 *
 */
public class UserNavigationSpout extends BaseRichSpout {
	private static final Logger LOGGER = LoggerFactory.getLogger(UserNavigationSpout.class);
	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try{
			this.host = conf.get("redis-host").toString();
			this.port = JStormUtils.parseInt(conf.get("redis-port"));
			this.collector = collector;
			//
			reconnect();
		}
		catch(Exception exp){
			LOGGER.error(" UserNavigationSpout prepare throw exception " , exp);
		}		
	}
	
	private void reconnect(){
		jedis = new Jedis(host,port);
	}

	@Override
	public void nextTuple() {
		try{
			String content = jedis.rpop("navigation");
			if(content == null || "nil".equals(content)){
				try{
					Thread.sleep(300);
				}catch(InterruptedException exp){
					LOGGER.error(" redis queue is empty! but sleep 300 throw exception!!!");
				}
			}
			else{
				JSONObject json = (JSONObject)JStormUtils.from_json(content);
				String user = json.get("user").toString();
				String product = json.get("product").toString();
				String type = json.get("type").toString();
				Map<String,String> map = new HashMap<>();
				map.put("product", product);
				NavigationEntry entity =  new NavigationEntry(user, type, map);
				// 
				collector.emit(new Values(user,entity));
			}
		}
		catch(Exception exp){
			LOGGER.error(" UserNavigationSpout nextTuple throw exception" , exp);
		}		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user","otherdata"));
	}

}
