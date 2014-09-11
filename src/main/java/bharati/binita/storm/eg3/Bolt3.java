package bharati.binita.storm.eg3;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import bharati.binita.storm.eg.tuple.model.Example1;

/**
 * 
 * @author binita.bharati@gmail.com
 * Receives the output from Bolt2.
 *
 */

public class Bolt3 implements IRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(Bolt3.class);
	
	private String field1;
	
	private String fieldGrpingKey;
	
	private LinkedHashMap<String, Integer> counter;
	
	public Bolt3(String field1, String fieldGrpingKey)
	{
		this.field1 = field1;
		this.fieldGrpingKey = fieldGrpingKey;
		this.counter = new LinkedHashMap<String, Integer>();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		Example1 fieldSpecificJmsMsg = (Example1)input.getValueByField("entire-msg");
		String counterKey = field1 + "-" + fieldSpecificJmsMsg.getField2() + "-" + fieldSpecificJmsMsg.getField3();
		if(counter.containsKey(counterKey))
		{
			counter.put(counterKey, counter.get(counterKey) + 1);
		}
		else
		{
			counter.put(counterKey, 1);
		}
		
		logger.info("Bolt3 : execute, fieldGrpingKey = "+fieldGrpingKey+", counter = "+counter);
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
