package bharati.binita.storm.eg1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//import org.apache.log4j.Logger;

public class HelloWorldBolt extends BaseRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(HelloWorldBolt.class);
	//private static final Logger logger = Logger.getLogger(HelloWorldBolt.class);
	
	private int myCount = 0;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		
		// TODO Auto-generated method stub
		String test = input.getStringByField("sentence");
		logger.info("execute: entered with "+test);
		 if("Hello World".equals(test))
		 {
			 myCount++;
			 logger.info("Found a Hello World! My Count is now: "
	                             + myCount);
		 }
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
