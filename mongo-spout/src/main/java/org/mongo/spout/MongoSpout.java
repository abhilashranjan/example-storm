package org.mongo.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class MongoSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1680013915228701189L;
//	private MongoClient client = new MongoClient();
	private LinkedBlockingQueue<DBObject> queue = new LinkedBlockingQueue<DBObject>();
	private SpoutOutputCollector collector;
//	private TopologyContext context;
//	private DBCollection collection;

	public MongoSpout() {

	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
//		this.context = context;
		MongoClient client = new MongoClient();
		DBCollection collection = client.getDB("users").getCollection("login");

		final DBCursor cur = collection
				.find()
				.sort(BasicDBObjectBuilder.start("$natural", 1).get())
				.addOption(
						Bytes.QUERYOPTION_TAILABLE
								| Bytes.QUERYOPTION_AWAITDATA);
		Thread t1 = new Thread(new Runnable() {

			public void run() {
				while (cur.hasNext()) {
					DBObject obj = cur.next();
					queue.add(obj);
					System.out.println("CURSOR   OBJECT   :-"+obj);
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		t1.start();
	}

	public void nextTuple() {
		if (!queue.isEmpty()) {
			
			DBObject obj = queue.poll();
			System.out.println("OBJECT "+obj);
			collector.emit(new Values(obj));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("temp"));

	}

}
