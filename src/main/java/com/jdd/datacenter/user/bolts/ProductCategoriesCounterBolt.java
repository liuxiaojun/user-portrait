package com.jdd.datacenter.user.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

public class ProductCategoriesCounterBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProductCategoriesCounterBolt.class);
	private Jedis jedis;
	// item:category ---> count
	private Map<String,Integer> counter = new HashMap<>();
	// 
	private Map<String,Integer> pendingToSave = new HashMap<>();
	private Timer timer;
	private OutputCollector collector;
	private String host;
	private int port;
	private long downloadTime;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.host = stormConf.get("redis-host").toString();
		this.port = JStormUtils.parseInt(stormConf.get("redis-port"));
		this.downloadTime = JStormUtils.parseLong(stormConf.get("download-time"));
		startDownloaderThread();
		this.collector = collector;
		reconnect();
	}
	
	private void reconnect(){
		this.jedis = new Jedis(host , port);
	}
	
	public int getProductCategoryCount(String categ , String product){
		Integer count = counter.get(buildKey(categ , product));
		if(count == null){
			String scount = jedis.hget(buildRedisKey(product), categ);
			if(scount == null || "nil".equals(scount)){
				count = 0;
			}
			else{
				count = JStormUtils.parseInt(scount);
			}
		}		
		return count;
	}

	private String buildRedisKey(String product){
		return "product:"+product;
	}
	
	private String buildKey(String categ, String product){
		return product + ":" + categ;
	}
	
	private void storeProductCategoryCount(String categ , String product, int count){
		String key = buildKey(categ,product);
		counter.put(key, count);
		synchronized(pendingToSave){
			pendingToSave.put(key, count);
		}
	}
	
	private int count(String product , String categ){
		int count = getProductCategoryCount(categ, product);
		count++;
		storeProductCategoryCount(categ,product,count);
		return count;
	}
	
	private void startDownloaderThread(){
		TimerTask task = new TimerTask(){
			@Override
			public void run() {
				Map<String, Integer> pendings;
				synchronized(pendingToSave){
					pendings = pendingToSave;
					pendingToSave = new HashMap<>();
				}
				
				for(String key : pendings.keySet()){
					String[] keys = key.split(":");
					String product = keys[0];
					String categ = keys[1];
					Integer count = pendings.get(key);
					jedis.hset(buildRedisKey(product), categ, count.toString());
				}
			}
			
		};
		timer = new Timer("Item categories downloader");
		timer.scheduleAtFixedRate(task, downloadTime, downloadTime);
	}
	
	@Override
	public void execute(Tuple input) {
		String product = input.getString(0);
		String categ = input.getString(1);
		int total = count(product,categ);
		
		collector.emit(new Values(product, categ, total));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("product","categ","visits"));
	}

	@Override
	public void cleanup(){
		super.cleanup();
		timer.cancel();
	}
}
