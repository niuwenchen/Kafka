package com.jackniu.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleHLConsumer {
	private final ConsumerConnector consumer;
	private final String topic;
	
	
	public SimpleHLConsumer(String zookeeper,String groupId,String topic)
	{
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
		this.topic=topic;
		
	}
	private ConsumerConfig createConsumerConfig(String zookeeper,String groupId)
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "2500");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
	
	public void testConsumer()
	{
		Map<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put(topic, new Integer(1));
		
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerStreamMap=
				consumer.createMessageStreams(topicMap);
		
		List<KafkaStream<byte[],byte[]>> streamList = consumerStreamMap.get(topic);
		for(final KafkaStream<byte[],byte[]> stream:streamList)
		{
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while(consumerIte.hasNext())
			{
				System.out.println("Message from Single Topic ::"
						+ new String(consumerIte.next().message()));
			}
			
		}
		if(consumer !=null)
		{
			consumer.shutdown();
		}
	}
	
	
	public static void main(String[] args) {
		String zookeeper = "192.168.222.128:2181";
		String groupId = "test-consumer-group";
		String topic = "kafkatopic";
		SimpleHLConsumer simpleHlConsumer = new SimpleHLConsumer(zookeeper, groupId, topic);
		simpleHlConsumer.testConsumer();
		                        

	}

}
