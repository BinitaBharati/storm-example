package bharati.binita.storm.eg.amq.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;

import bharati.binita.storm.eg.tuple.model.Example1;

import clojure.main;

/**
 * 
 * @author binita.bharati@gmail.com
 * Converts Object message into a String, and sends via wire as a TextMessage. (No serialization overhead )
 * Clojure used to convert Object msg to Text msg.
 * 
 * One of the fields in the Object message will be identical for all msges - to demonstrate storm segregation capability.
 * (Run this class more than once with different values for 'identicalFieldValue')
 *
 */

public class MsgProducer1 {
	
	private ConnectionFactory connectionFactory;
	private String queueName;
	
	//msg attributes
	private int msgCount;
	private int msgStartIdx;
	
	private List<String> field1List;
	private List<String> field2List;
	private List<String> field3List;
	
	private LinkedHashMap<String, Integer> counter;
	
	
	
	public MsgProducer1(String jmsBrokerUrl1, String queue1, int msgCount1, 
			int msgStartIdx, String field1Vals, String field2Vals, String field3Vals) throws Exception {
		// TODO Auto-generated constructor stub
		
		this.connectionFactory = new ActiveMQConnectionFactory(jmsBrokerUrl1);
		this.queueName = queue1;
				
		this.msgCount = msgCount1;	
		this.msgStartIdx = msgStartIdx;
		
		StringTokenizer st = new StringTokenizer(field1Vals,";");
		this.field1List = new ArrayList<String>();
		while (st.hasMoreTokens())
		{
			field1List.add(st.nextToken());
		}
		
		st = new StringTokenizer(field2Vals,";");
		field2List = new ArrayList<String>();
		while (st.hasMoreTokens())
		{
			field2List.add(st.nextToken());
		}
		
		st = new StringTokenizer(field3Vals,";");
		field3List = new ArrayList<String>();
		while (st.hasMoreTokens())
		{
			field3List.add(st.nextToken());
		}
		
		counter = new LinkedHashMap<String, Integer>();
		
	}
	
	public void produceMsg() throws Exception
	{
		Connection conn = connectionFactory.createConnection();
		Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Queue queue = session.createQueue(queueName);
		
		MessageProducer producer = session.createProducer(queue);
		
		Random rnd = new Random();
		
		msgStartIdx++;
		
		for( int k= 0 ; k < field1List.size() ; k++)
		{
			int idx = msgStartIdx;
			
			for(int i = 0 ; i < this.msgCount ; i++)
	        {
	        
				sendTxtMsg(session, producer, idx, rnd, field1List.get(k));
				idx++;
	        }
		}
		
		
		producer.close();
		session.close();
		conn.close();
		
		System.out.println("Generated the following msg count : "+counter);
		 
		
	}
	
	private void sendTxtMsg( Session session, MessageProducer producer,
			int msgStartIdx, Random rnd, String field1) throws Exception
	{
		Example1 exMsg = generateTuple(msgStartIdx, rnd, field1);
		final String exMsgStr =  
				(String)EvalUtil.invokeVar("msg_util.clj", "msg-util", "get-example-str-msg1", ReflectionUtil.toMap(exMsg));
	   System.out.println("produceMsges: after generating exMsgStr - "+exMsgStr);
		
		TextMessage msg = session.createTextMessage();
		msg.setText(exMsgStr);
		
		producer.send(msg);
		
		
	}
	
	private Example1 generateTuple(int msgStartIdx, Random rnd, String field1)
	{
		
		Example1 msg = new Example1();
		msg.setId(msgStartIdx+"");
		msg.setField1(field1);
		msg.setField2(field2List.get(rnd.nextInt(4)));
		msg.setField3(field3List.get(rnd.nextInt(4)));
		msg.setField4("FIELD4-"+msgStartIdx);
		
		String counterKey = field1+"-"+msg.getField2()+"-"+msg.getField3();
		
		if(counter.get(counterKey) != null)
		{
			counter.put(counterKey, counter.get(counterKey) + 1);
		}
		else
		{
			counter.put(counterKey, 1);
		}
		
		    
		return msg;
				   
	}
	
	 //args[0] - totalMsgCount
	 //args[1] - msgStartIdx
	public static void main(String[] args) throws Exception {
		MsgProducer1 temp = new MsgProducer1("tcp://192.168.1.2:61616?wireFormat.maxInactivityDuration=3600000",
				"bintest",Integer.parseInt(args[0]),Integer.parseInt(args[1]), "bluemoon;redrose","aries;leo;gemini;virgo",
				"pather;lion;leopard;tiger");
		temp.produceMsg();
		
		
		
	}

}
