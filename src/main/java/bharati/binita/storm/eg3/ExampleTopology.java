package bharati.binita.storm.eg3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import clojure.main;

/**
 * 
 * @author binita.bharati@gmail.com
 * args[0] = amq provider url
 * args[1] = queue name
 * args[2] - topologyName
 * args[3] - <field1> list (semicolon seperated string) Eg : bluemoon;redrose
 * args[4] - noOfWorkers - 4 workers run on default ports : 6700 to 6703
 * args[5] - parallelism hint per <field1> bolt. Eg : bluemoon=3;redrose=5; ie actually mapped to no of executor threads per spout/bolt.
 * 
 * 
 * zip -d storm-example-0.1.0-SNAPSHOT-standalone.jar defaults.yaml
 * 
 * bin/storm jar /Users/bbharati/personal/projects/eclipseWS/storm-example/target/storm-example-0.1.0-SNAPSHOT-standalone.jar bharati.binita.storm.eg.ExampleTopology
 * "tcp://192.168.1.2:61616?wireFormat.maxInactivityDuration=3600000" "bintest" "example-topo" "bluemoon;redrose" 8 "bluemoon=3;redrose=5"
 * 
 * AMQ version = 5.5.1, storm version = apache-storm-0.9.2-incubating
 *
 */

public class ExampleTopology {
	
	private static Logger logger = LoggerFactory.getLogger(ExampleTopology.class);
	
	public static void main(String[] args) throws Exception {
		
		//Extract field1 list	
	     List<String> field1List = new ArrayList<String>();
	    
	     StringTokenizer st = new StringTokenizer(args[3],";");
			
		while (st.hasMoreTokens())
		{
			field1List.add(st.nextToken());
		}
		logger.info("field1List = "+field1List);
		
		//Extract parallelismHintMap
		Map<String, Integer> parallelismHintMap = new HashMap<String, Integer>();
		StringTokenizer st1 = new StringTokenizer(args[5],";");
		logger.info("parallelismHintMapArg = "+args[5]);
		
		while (st1.hasMoreTokens())
		{
			String temp = st1.nextToken();
			logger.info("temp = "+temp);
			parallelismHintMap.put(temp.substring(0, temp.indexOf("=")), Integer.parseInt(temp.substring(temp.lastIndexOf("=")+1)));
		}
		logger.info("parallelismHintMap = "+parallelismHintMap);
		
		//Topology definition
		/**
		 * (Each Bolt2 and Bolt3 instance may run in 1 or more executor thread as specified by parallelismHintMap)
		 * 
		 * 											  field1 =					 	field1=bluemoon
		 *                                            bluemoon stream  				field2=puppy
		 * ActiveMQSpout -> SegreggatorBolt ----------------------------> Bolt2 ---------------------------> Bolt3
		 *                                          | 							|
		 *                                          |							|    field1=bluemoon
		 *                                          |							|	 field2=kitten
		 *                                          |							|---------------------------> Bolt3
		 *                                          |
		 *                                          | field1 =						field1=redrose
		 *                                          | redrose stream				field2=kid
		 *                                          |-------------------> Bolt2 -----------------------------> Bolt3
		 *                                          							|
		 *                                          							|	field1=redrose
		 *                                          							|	field3=cub
		 *                                          							|----------------------------> Bolt3
		 *                                          							|
		 *                                       								|	field1=redrose
		 *                                         								|	field3=xyz
		 * 																		|-----------------------------> Bolt3
		 * 
		 */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("amq-spout",new ActiveMQSpout());
        builder.setBolt("segreggator-bolt", new Bolt1(field1List)).shuffleGrouping("amq-spout");
        
        for(String eachField1 : field1List)
        {
        	 //builder.setBolt(eachField1+"-bolt2", new Bolt2(eachField1), parallelismHintMap.get(eachField1)).shuffleGrouping("segreggator-bolt", eachField1+"-stream");
        	builder.setBolt(eachField1+"-bolt2", new Bolt2(eachField1)).shuffleGrouping("segreggator-bolt", eachField1+"-stream");
        	 
        	 List<String> tupleList = new ArrayList<String>();
      		 tupleList.add("field2");
      		 tupleList.add("field3");
      		
      		for(String eachFieldGrp : tupleList)
      		{
      			/*builder.setBolt(eachField1+"-"+eachFieldGrp+"-bolt3", new Bolt3(eachField1, eachFieldGrp),
            			 parallelismHintMap.get(eachField1))
            		.fieldsGrouping(eachField1+"-bolt2", new Fields(eachFieldGrp));*/
      			builder.setBolt(eachField1+"-"+eachFieldGrp+"-bolt3", new Bolt3(eachField1, eachFieldGrp))
           		.fieldsGrouping(eachField1+"-bolt2", new Fields(eachFieldGrp));
      		}
        }
        
        //Configuration
        Config conf = new Config();
        conf.put("brokerUrl", args[0]);
        conf.put("queue", args[1]);
        conf.put("field1List", field1List);
        conf.setDebug(false);
        
        conf.setNumWorkers(Integer.parseInt(args[4]));
	    StormSubmitter.submitTopology(args[2], conf,
	    builder.createTopology());
		
	}

}
