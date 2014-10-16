package bharati.binita.storm.eg4;

import java.util.Map;
import java.util.UUID;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author binita.bharati@gmail.com
 * DumbBolt - Just prints and passes on the tuple forward.
 *
 */



public class DumbBolt extends BaseRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(DumbBolt.class);
    
	private OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		
		// TODO Auto-generated method stub
		String word = input.getStringByField("word");
		logger.info("execute: entered with "+word);
		UUID msgId = UUID.randomUUID();
		//collector.emit(input, new Values(word));//Anchored tuple
		collector.emit(new Values(word));//Unanchored tuple
		this.collector.ack(input);
		
		 
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));

		
	}

}
