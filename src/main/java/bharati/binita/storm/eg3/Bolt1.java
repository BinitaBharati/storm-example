package bharati.binita.storm.eg3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import bharati.binita.storm.eg.amq.client.EvalUtil;

/**
 * 
 * @author binita.bharati@gmail.com
 * This is the first bolt that receives multiple <field1> data Iin the form of String) from the ActiveMQSpout
 * and segregates per <field1> data to <field1>-specific stream.
 * It will emit the recieved String type msg as it is.
 * The string type messages is actually a key value pair ( Basically a stringified hashmap)
 *
 */

public class Bolt1 implements IRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(Bolt1.class);
	
	private OutputCollector collector;
	
	private List<String> feild1List;
	
	public Bolt1(List<String> field1List)
	{
		this.feild1List = field1List;
		logger.info("Bolt1: fieldId1List = "+field1List);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
		logger.info("prepare: fieldId1List = "+feild1List);
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String eachJmsMsg = (String)input.getValueByField("jms-msg");
		logger.info("Bolt1 - execute: entered with "+eachJmsMsg);
		try {
			String field1 =  
					(String)EvalUtil.invokeVar("msg_util.clj", "msg-util", "get-msg-attr", eachJmsMsg, "field1");
			logger.info("Bolt1 - execute: fieldId1 "+field1);
			//if(eventCompanyId.equals(companyId))
			if(feild1List.contains(field1))
			{
				logger.info("execute: yeaaa!!, matching msg found for field1 = "+field1);
				
				collector.emit(field1+"-stream", input.getValues());
								
			}
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
		//declarer.declare(new Fields("eachCompanyMsg"));
		logger.info("declareOutputFields: fieldId1List = "+feild1List);
		for(String eachFieldId1 : feild1List)
		{
			declarer.declareStream(eachFieldId1+"-stream", new Fields("bolt1-op"));
		}
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
