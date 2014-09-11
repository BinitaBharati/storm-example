package bharati.binita.storm.eg3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import bharati.binita.storm.eg.amq.client.EvalUtil;
import bharati.binita.storm.eg.amq.client.ReflectionUtil;
import bharati.binita.storm.eg.tuple.model.Example1;

/**
 * 
 * @author binita.bharati@gmail.com
 * Listens to specific <field1> specific stream emitted by Bolt1.
 * Emits field grouping related tuples (entire msg + field2 + field3 ), 
 * so that Bolt3 instances can field group based on the value of field2 or field3 , 
 * and still retrieve the entire message.
 *
 */

public class Bolt2 implements IRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(Bolt2.class);
	
	private OutputCollector collector;
	
	private String field1;
	
	public Bolt2(String field1)
	{
		this.field1 = field1;
	}


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		String eachJmsMsg = (String)input.getValueByField("bolt1-op");
		try {
			String field1 =  
					(String)EvalUtil.invokeVar("msg_util.clj", "msg-util", "get-msg-attr", eachJmsMsg, "field1");
			logger.info("Bolt2 - execute: entered for field1 = "+field1);
			String field2 =  
					(String)EvalUtil.invokeVar("msg_util.clj", "msg-util", "get-msg-attr", eachJmsMsg, "field2");
			String field3 =  
					(String)EvalUtil.invokeVar("msg_util.clj", "msg-util", "get-msg-attr", eachJmsMsg, "field3");
			
			Map<String, Object> map = (Map<String, Object>)EvalUtil.invokeVar("msg_util.clj", "msg-util", "read-str-to-map", eachJmsMsg);
			
			Example1 eg = (Example1)ReflectionUtil.fromMap(map, Example1.class);
			
			collector.emit(new Values(eg, field2, field3));
			
			logger.error("Bolt2 - exiting");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		List<String> tupleList = new ArrayList<String>();
		tupleList.add("entire-msg");
		tupleList.add("field2");
		tupleList.add("field3");
		declarer.declare(new Fields(tupleList));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
