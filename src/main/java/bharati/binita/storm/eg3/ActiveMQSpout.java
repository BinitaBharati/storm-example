package bharati.binita.storm.eg3;

import java.util.Map;


import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

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
 * Listens to String messages in AMQ , and emits them as it is.
 * The string type messages is actually a key value pair ( Basically a stringified hashmap)
 *
 */

public class ActiveMQSpout implements IRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(ActiveMQSpout.class);
	
	private Connection conn;
	private Session sess;
	private MessageConsumer cons;
	
	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
		this.collector = collector;
		
		String brokerUrl = (String)conf.get("brokerUrl");
		String queueName = (String)conf.get("queue");
		
		
		ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory(brokerUrl);
		
	    try {

	    	conn = connFactory.createConnection("admin", "admin");
	    	conn.start();
		
	    	sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
	    	Queue queue = sess.createQueue(queueName);
		
	    	cons = sess.createConsumer(queue);
	    	
	    } catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
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
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		try {
			cons.setMessageListener(new AMQMsgListener());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("jms-msg"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public class AMQMsgListener implements MessageListener
	{

		@Override
		public void onMessage(Message msg) {
			// TODO Auto-generated method stub
			
			TextMessage tm = (TextMessage)msg;
			String text = null;
			try {
				text = tm.getText();
				logger.info("nextTuple -> onMessage : got "+text);
				
				collector.emit(new Values(text));
			} catch (JMSException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
		}
		
	}

}
