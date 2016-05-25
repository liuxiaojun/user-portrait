package com.jdd.datacenter.user.bolts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

/**
 * user history
 * @author hadoop
 *
 */
public class UserHistoryBolt extends BaseRichBolt {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(UserHistoryBolt.class);
	private OutputCollector collector;
	private String host;
	private int port;
	private Jedis jedis;
		
	private void reconnect(){
		this.jedis = new Jedis(host , port);
	}
	
	private Map<String,Set<String>> userNavigatedItems = new HashMap<>();
	
	
	private void addProductToHistory(String user,String product){
		Set<String> userHistory = getUserNavigationHistory(user);
		userHistory.add(product);
		jedis.sadd(buildKey(user), product);
	}

	private Set<String> getUserNavigationHistory(String user){
		Set<String> userHistory = userNavigatedItems.get(user);
		if(userHistory == null){
			userHistory = jedis.smembers(buildKey(user));
			if(userHistory == null){
				userHistory = new HashSet<>();
			}
			userNavigatedItems.put(user, userHistory);
		}
		return userHistory;
	}
	
	private String buildKey(String user){
		return "history:"+user;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("product","categ"));
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		try {
			this.collector = collector;
			this.host = conf.get("redis-host").toString();
			this.port = JStormUtils.parseInt(conf.get("redis-port"));
			reconnect();
		} catch (Exception e) {
			LOGGER.error(" UserHistoryBolt prepare throw exception " , e);
		}
	}
	@Override
	public void execute(Tuple input) {
		String user = input.getString(0);
		String prod1 = input.getString(1);
		String cat1 = input.getString(2);
		
		Set<String> productNavigated = getUserNavigationHistory(user);
		
		String prodKey = prod1 + ":" + cat1;
		if(!productNavigated.contains(prodKey)){
			for(String other : productNavigated){
				String[] ot = other.split(":");
				String prod2 = ot[0];
				String cat2 = ot[1];
				
				collector.emit(new Values(prod1,cat2));
				collector.emit(new Values(prod2,cat1));
			}
			addProductToHistory(user,prodKey);
		}
	}

}
