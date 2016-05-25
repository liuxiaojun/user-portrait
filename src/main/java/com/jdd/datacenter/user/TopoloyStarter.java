package com.jdd.datacenter.user;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import com.jdd.datacenter.user.bolts.GetCategoryBolt;
import com.jdd.datacenter.user.bolts.NewsNotifierBolt;
import com.jdd.datacenter.user.bolts.ProductCategoriesCounterBolt;
import com.jdd.datacenter.user.bolts.UserHistoryBolt;
import com.jdd.datacenter.user.spout.UserNavigationSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * topology main class
 * @author hadoop
 *
 */
public class TopoloyStarter {
	public static final Logger LOGGER = LoggerFactory.getLogger(TopoloyStarter.class);
	public static final String REDIS_HOST = "10.33.97.246";
	public static final String REDIS_PORT = "19000";
	public static final String WEBSERVER = "http://10.33.96.28:3000/news";
	public static final long DOWNLOAD_TIME = 100;
	public static boolean testing = false;
	public static Map config = new HashMap();
	public static String TOPOLOGY_NAME="analytics";
	public static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
	public static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
	
	
	/**
	 * execute topology
	 * @param builder
	 * @param config
	 */
	public static void topoloyBuilder(TopologyBuilder builder , Map conf){
		int spout_parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT));
		int bolt_parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT));
		// spout
		builder.setSpout("read-feed", new UserNavigationSpout(),spout_parallelism_hint);
		// bolt
		builder.setBolt("get-categ", new GetCategoryBolt(),spout_parallelism_hint).shuffleGrouping("read-feed");
		builder.setBolt("user-history", new UserHistoryBolt(),bolt_parallelism_hint).fieldsGrouping("get-categ",new Fields("user"));
		builder.setBolt("product-categ-counter", new ProductCategoriesCounterBolt(),bolt_parallelism_hint).fieldsGrouping("user-history",new Fields("product"));
		
		if(!testing){
			builder.setBolt("news-notifier", new NewsNotifierBolt(),bolt_parallelism_hint).shuffleGrouping("product-categ-counter");
		}
		
		conf.put(Config.TOPOLOGY_WORKERS, conf.get(Config.TOPOLOGY_WORKERS));
		conf.put("redis-host", REDIS_HOST);
		conf.put("redis-port",REDIS_PORT);
		conf.put("webserver",WEBSERVER);
		conf.put("download-time",DOWNLOAD_TIME);
	}
	/**
	 * config file
	 * @param path
	 * @return
	 */
	public static Map loadConfig(String path){
		if(path.endsWith(".yaml")){
			return LoadConf.LoadYaml(path);
		}
		else if(path.endsWith(".properties")){
			return LoadConf.LoadProperty(path);
		}
		else{
			LOGGER.warn(" assigned config file format is unknown , please check file whether not exists or file format is wrong!!!");
		}
		return null;
	}
	
	private static void executeDistributed() throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();		
		topoloyBuilder(builder , config);	
		
		StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
	}
	private static void executeLocal() {
		TopologyBuilder builder = new TopologyBuilder();
		topoloyBuilder(builder , config);
		
		LocalCluster local = new LocalCluster();
		local.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length < 1){
			LOGGER.warn(" config file is not assigned!!! ");
			return;
		}
		config = loadConfig(args[0]);
		if(config != null){
			if(StormConfig.local_mode(config)){
				executeLocal();
			}
			else{
				executeDistributed();
			}
		}
	}
	
	
}
