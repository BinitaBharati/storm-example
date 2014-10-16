package bharati.binita.storm.eg4;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class ExceptionRaiserBolt extends BaseRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(ExceptionRaiserBolt.class);
	
	private OutputCollector collector;
	
	private Random random;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.random = new Random();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String word = input.getStringByField("word");
		String curThread = Thread.currentThread().getName();
		Util.logMessage(logger, curThread, "execute: entered for word = %s", word);
		int temp = random.nextInt(3);//will generate either 0/1/2
		logger.info("execute: temp = "+temp);

		if(temp == 0)
		{
			//success scenario
			this.collector.ack(input);

			
		}
		else if(temp == 1)
		{
			Util.logMessage(logger, curThread, "execute: throwing RunTimeException for word = %s", word);

			//Throws RunTime Exception - The Thread processing the tuple wont die, it wil just continue processing the nextTuple.
			int a = 1/0;
		}
		else//temp = 2
		{
			
			Util.logMessage(logger, curThread, "execute: doing timepass for word = %s", word);
			
			timepass(3*60*1000);
			
			Util.logMessage(logger, curThread, "execute: after doing timepass for word = %s", word);


		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	public static void timepass(long sleepMillis) {

	    try {
			Thread.sleep(sleepMillis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
