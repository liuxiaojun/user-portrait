package com.jdd.datacenter.user.model;

import org.json.simple.JSONObject;

import com.alibaba.jstorm.utils.JStormUtils;

import redis.clients.jedis.Jedis;

/**
 * 
 * @author hadoop
 *
 */
public class ProductsReader {
	private String host;
	private int port;
	private Jedis jedis;
	
	public ProductsReader(String host, int port) {
		super();
		this.host = host;
		this.port = port;
		reconnect();
	}
	
	private void reconnect(){
		jedis = new Jedis(host,port);
	}
	
	public Product readItem(String id) {
		String content = jedis.get(id);
		if(content == null || "nil".equals(content)){
			return null;
		}
		JSONObject json = (JSONObject)JStormUtils.from_json(content);
		Product product = new Product(JStormUtils.parseLong(json.get("id")),
									  json.get("title").toString(),
									  JStormUtils.parseDouble(json.get("price")),
									  json.get("category").toString());
		return product;
	}
}
