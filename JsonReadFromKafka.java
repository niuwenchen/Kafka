package com.jackniu.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class JsonReadFromKafka {
	private final ConsumerConnector consumer;
		
		public JsonReadFromKafka() throws Exception
		{
				Properties props = new Properties();
				props.load(new FileInputStream("D:\\software\\eclipsemars\\svn1\\kafkaClient\\mytestconsumer.properties"));
				
				ConsumerConfig config = new ConsumerConfig(props);
				consumer = (ConsumerConnector) Consumer.create(config);
				System.out.println(consumer);
				
		}
		public void shutdown()
		{
			if(consumer != null)
				consumer.shutdown();			
		}
		
		public void consume()
		{
			System.out.println("run");
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put("testmysql", new Integer(1));
	        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
	        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

	        Map<String, List<KafkaStream<String, String>>> consumerMap = 
	        consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
	        System.out.println(consumerMap.size());
	        KafkaStream<String, String> stream = consumerMap.get("testmysql").get(0);
	        ConsumerIterator<String, String> it = stream.iterator();
	        while (it.hasNext())
	            System.out.println(it.next().message());
		}
		
		public static  void main(String[] args) throws Exception
		{
			new JsonReadFromKafka().consume();
			
		}
		
		

}
