package org.mongo.spout;

import java.util.Map;

import com.mongodb.DBObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MongoBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 420429704284232881L;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	public void execute(Tuple input) {
		System.out.println("BOLT :-"+input.getValueByField("temp"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
