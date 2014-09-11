package bharati.binita.storm.eg1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import clojure.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldTopology {
	
	private static Logger logger = LoggerFactory.getLogger(HelloWorldTopology.class);
	
	public static void main(String[] args) throws Exception, InvalidTopologyException {
		logger.info("main: entered with "+args);
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("randomHelloWorld", new
                HelloWorldSpout(), 3);
		builder.setBolt("HelloWorldBolt", new
               HelloWorldBolt(), 2)
               .shuffleGrouping("randomHelloWorld");
		Config conf = new Config();
		conf.setDebug(true);//tto many logs will come if set to true
		if(args!=null && args.length > 0) {
			    conf.setNumWorkers(3);
			    StormSubmitter.submitTopology(args[0], conf,
			     builder.createTopology());
			} else {
			    LocalCluster cluster = new LocalCluster();
			    cluster.submitTopology("test", conf,
			                          builder.createTopology());
			    Utils.sleep(10000);
			    cluster.killTopology("test");
			    cluster.shutdown();
			}
	}

}
