package bharati.binita.storm.eg2;

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

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */

public class WordNormalizer implements IRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(WordNormalizer.class);
	
	private OutputCollector collector;

    public void cleanup() {}

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     *
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    public void execute(Tuple input) {
    String sentence = input.getString(0);
    logger.info("execute: enetred with sentence = "+sentence);
    String[] words = sentence.split(" ");
    for(String word : words){
        word = word.trim();
        if(!word.isEmpty()){
            word = word.toLowerCase();
            //Emit the word
            List a = new ArrayList();
            a.add(input);
            collector.emit(a,new Values(word));
        }
    }
    // Acknowledge the tuple
    collector.ack(input);
}
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) {
    		logger.info("WordNormalizer: entered prepare");
            this.collector = collector;
    }

    /**
     * The bolt will only emit the field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
    }

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
