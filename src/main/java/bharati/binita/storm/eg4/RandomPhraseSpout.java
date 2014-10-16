package bharati.binita.storm.eg4;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

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

import org.apache.commons.io.IOUtils;



public class RandomPhraseSpout extends BaseRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseSpout.class);
	//private static final Logger logger = Logger.getLogger(HelloWorldSpout.class);
	
	private String[] sentences;
	
	private SpoutOutputCollector collector;
	
	private Random random;
	
	private int MAX_RANDOM;
	
	public RandomPhraseSpout(){
        
      }

	/**
	 * open is like a init method - is called in the beginning when Spout is initialized.
	 * This can be used to set spout config.
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.random = new Random();
		try {
			sentences = (String[]) IOUtils.readLines(
			    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		MAX_RANDOM = sentences.length;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		logger.info("nextTuple: entered");
		String emitWord = sentences[random.nextInt(MAX_RANDOM)];
		UUID msgId = UUID.randomUUID();
		logger.info("nextTuple: emitting word = "+emitWord +", msgId = "+msgId);
		collector.emit(new Values(emitWord), msgId);
		
        
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		logger.info("fail: msgId = "+msgId);
		super.fail(msgId);
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		logger.info("ack: msgId = "+msgId);
		super.ack(msgId);
	}

}
