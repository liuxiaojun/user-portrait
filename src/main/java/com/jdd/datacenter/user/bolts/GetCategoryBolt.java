package com.jdd.datacenter.user.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.jdd.datacenter.user.model.NavigationEntry;
import com.jdd.datacenter.user.model.Product;
import com.jdd.datacenter.user.model.ProductsReader;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * get category
 * @author hadoop
 *
 */
public class GetCategoryBolt extends BaseBasicBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(GetCategoryBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ProductsReader reader;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf,TopologyContext context){
		try {
			String host = conf.get("redis-host").toString();
			int	port = JStormUtils.parseInt(conf.get("redis-port"));
			this.reader = new ProductsReader(host,port);
			super.prepare(conf, context);
		} catch (Exception e) {
			LOGGER.error(" GetCategoryBolt prepare throw exception " , e);
		}
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		NavigationEntry entity = (NavigationEntry)input.getValue(1);
		if("PRODUCT".equals(entity.getPageType())){			
			try {
				String product = (String)entity.getOtherData().get("product");
				Product item = reader.readItem(product);
				if(item == null){
					return;
				}
				String category = item.getCategory();
				collector.emit(new Values(entity.getUserId(),product,category));
			} catch (Exception e) {
				LOGGER.error(" GetCategoryBolt execute throw exception " , e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user","product","categ"));
	}

}
