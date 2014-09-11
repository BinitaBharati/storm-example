package bharati.binita.storm.eg1;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.log4j.Logger;


public class HelloWorldSpout extends BaseRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(HelloWorldSpout.class);
	//private static final Logger logger = Logger.getLogger(HelloWorldSpout.class);
	
	private SpoutOutputCollector collector;
	
	private int referenceRandom;
	
	private static final int MAX_RANDOM = 10;
	
	public HelloWorldSpout(){
        final Random rand = new Random();
        referenceRandom = rand.nextInt(MAX_RANDOM);
      }

	/**
	 * open is like a init method - is called in the beginning when Spout is initialized.
	 * This can be used to set spout config.
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		logger.info("nextTuple: entered");
		Utils.sleep(100);
        final Random rand = new Random();
        int instanceRandom = rand.nextInt(MAX_RANDOM);
        if(instanceRandom == referenceRandom){
        	logger.info("nextTuple: emitting Hello World");
          collector.emit(new Values("Hello World"));
        } else {
        	logger.info("nextTuple: emitting Other Random Word");
          collector.emit(new Values("Other Random Word"));
        }
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
	}

}
