package org.mongo.spout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class MongoTopology {

	public static void main(String args[]) {

		Config conf = new Config();
		conf.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("mongospout", new MongoSpout());
		builder.setBolt("mongobolt", new MongoBolt()).shuffleGrouping("mongospout");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("mongotopology", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("mongotopology");
		cluster.shutdown();
		
	}
}
