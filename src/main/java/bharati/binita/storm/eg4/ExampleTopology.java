package bharati.binita.storm.eg4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


/**
 * 
 * @author binita.bharati@gmail.com
 * Demo of Storm's replay feature (Guaranteed processing)
 * https://storm.incubator.apache.org/documentation/Guaranteeing-message-processing.html
 * 
 * Rules on making ACK to work:
 * -Originating Spout should emit anchored tuple( i.e with a messageId)
 * -Originating Spout should implement ack method
 * -All the Bolts in the tuple tree should acknowledge the tuple using collector.ack method
 * 
 * Rules on making FAIL to work:
 * -conf.setMessageTimeoutSecs should be used to customize the Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS to desired value.
 * -Originating Spout should emit anchored tuple( i.e with a messageId)
 * -Originating Spout should implement fail method
 * -Subsequent Bolts should delay the execution of the tuple . Eg: By doing a Thread.sleep
 * -It is upon the user to decide the implementation of fail method. Eg- The failed tuple can be replayed etc.
 * 
 *
 */

public class ExampleTopology {
	
	private static Logger logger = LoggerFactory.getLogger(ExampleTopology.class);

	
	public static void main(String[] args) throws Exception, InvalidTopologyException {
		logger.info("main: entered with "+args);
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("randomWordSpout", new
                RandomPhraseSpout(), 3);
		builder.setBolt("dumbBolt", new
               DumbBolt(), 2)
               .shuffleGrouping("randomWordSpout");
		builder.setBolt("alarmBolt", new
	               ExceptionRaiserBolt(), 2)
	               .shuffleGrouping("dumbBolt");
		Config conf = new Config();
		//		Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS = "30"; //30 secs default, can't be customized as it is a final field.
		conf.setMessageTimeoutSecs(conf, 20);

		conf.setDebug(false);//too many logs will come if set to true
		StormSubmitter.submitTopology(args[0], conf,
			     builder.createTopology());
		
	}

}
