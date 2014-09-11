package bharati.binita.storm.eg2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */

public class WordReader implements IRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(WordReader.class);
	
	private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
    public boolean isDistributed() {return false;}
    public void ack(Object msgId) {
            logger.info("ack - OK:"+msgId);
    }
    public void close() {}
    public void fail(Object msgId) {
            logger.info("fail - FAIL:"+msgId);
    }

    /**
     * The only thing that the methods will do It is emit each
     * file line. Note: nextTuple will run indefinitely in a loop, till topoogy is killed.
     */
    public void nextTuple() {
            /**
             * The nextuple it is called forever, so if we have read the file
             * we will wait and then return
             */
        	logger.info("nextTuple: entered");

            if(completed){
                    try {
                            Thread.sleep(1000);
                    } catch (InterruptedException e) {
                            //Do nothing
                    }
                    logger.info("nextTuple: going to return from spout");
                    return;
            }
            String str;
            //Open the reader
            BufferedReader reader = new BufferedReader(fileReader);
            try{
                    //Read all lines
                    while((str = reader.readLine()) != null){
                            /**
                             * By each line emit a new value with the line as a their
                             */
                    	    logger.info("nextTuple: eachLine = "+str);
                            this.collector.emit(new Values(str),str);
                    }
            }catch(Exception e){
                    throw new RuntimeException("Error reading tuple",e);
            }finally{
                    completed = true;
            }
    }

    /**
     * We will create the file and get the collector object
     */
    public void open(Map conf, TopologyContext context,
                    SpoutOutputCollector collector) {
    	   logger.info("WordRedaer: open");
            try {
                    this.context = context;
                    this.fileReader = new FileReader(conf.get("wordsFile").toString());
            } catch (FileNotFoundException e) {
                    throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
            }
            this.collector = collector;
    }

    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
    }
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
