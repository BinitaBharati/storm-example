package bharati.binita.storm.eg2;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */

public class WordCounter implements IRichBolt {
	
	private static Logger logger = LoggerFactory.getLogger(WordCounter.class);


    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector collector;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     */
    @Override
    public void cleanup() {
            logger.info("cleanup: -- Word Counter ["+name+"-"+id+"] --");
            for(Map.Entry<String, Integer> entry : counters.entrySet()){
                    logger.info("cleanup: "+entry.getKey()+": "+entry.getValue());
            }
    }

    /**
     * On each word We will count
     */
    @Override
    public void execute(Tuple input) {
            String str = input.getString(0);
            logger.info("WordCounter: execute: str = "+str);
            /**
             * If the word dosn't exist in the map we will create
             * this, if not We will add 1
             */
            if(!counters.containsKey(str)){
                    counters.put(str, 1);
            }else{
                    Integer c = counters.get(str) + 1;
                    counters.put(str, c);
            }
            logger.info("WordCounter: execute: counter for word = "+str+ " = "+counters);
            //Set the tuple as Acknowledge
            collector.ack(input);
    }

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) {
    	    logger.info("WordCounter: prepare");
            this.counters = new HashMap<String, Integer>();
            this.collector = collector;
            this.name = context.getThisComponentId();
            this.id = context.getThisTaskId();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
