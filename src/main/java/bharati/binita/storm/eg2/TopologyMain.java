package bharati.binita.storm.eg2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 
 * @author binita.bharati@gmail.com
 * 
 * TopologyMain <topo name> <filepath>
 * File should be copied to all Supervisor m/cs.
 *
 */

public class TopologyMain {
	
		private static Logger logger = LoggerFactory.getLogger(TopologyMain.class);
        public static void main(String[] args) throws Exception {

        //Topology definition
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("word-reader",new WordReader());
                builder.setBolt("word-normalizer", new WordNormalizer())
                        .shuffleGrouping("word-reader");
                builder.setBolt("word-counter", new WordCounter(),2)
                        .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
                Config conf = new Config();
                conf.put("wordsFile", args[1]);
                conf.setDebug(false);
                
                conf.setNumWorkers(3);
			    StormSubmitter.submitTopology(args[0], conf,
			    builder.createTopology());
        //Topology run
               /* conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("Getting-Started-Toplogie", conf, 
                    builder.createTopology());
                Thread.sleep(1000);
                cluster.shutdown();*/
        }
}