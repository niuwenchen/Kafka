package com.jackniu.consumer;

/*
 * The Only differenceis that we first create a 
 * thread pool and get the Kafka streams associated with
 * each thread within the pool 
 * 
 * */
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MultiThreadConsumer {
	private ExecutorService executor;
	private final ConsumerConnector consumer;
	private final String topic;
	
	public MultiThreadConsumer(String zookeeper,String groupId,String topic)
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
	public void shutdown()
	{
		if(consumer != null)
			consumer.shutdown();
		if(executor != null)
			executor.shutdown();
	}
	public void testMultiThreadConsumer(int threadCount)
	{
		Map<String,Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, new Integer(threadCount));
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerStreamMap=
				consumer.createMessageStreams(topicMap);
		
		List<KafkaStream<byte[],byte[]>> streamList = consumerStreamMap.get(topic);
		
		executor = Executors.newFixedThreadPool(threadCount);
		int count =0;
		for(final KafkaStream<byte[],byte[]> stream:streamList )
		{
			final int threadNumber = count;
			executor.submit(new Runnable(){

				public void run() {
					ConsumerIterator<byte[], byte[]> iterator= stream.iterator();
					while(iterator.hasNext())
						System.out.println("Thread number "+threadNumber+": "
								+new String(iterator.next().message()));
					System.out.println("Shutting down Thread Number :"+ threadNumber);
					
					
				}			
			});
			count++;
		}
		
		
	}
	public static void main(String[] args)
	{
		String zookeeper = "192.168.222.128:2181";
		String groupId = "testgroup";
		String topic = "kafkatopic";
		int threadCount=2;
		MultiThreadConsumer multiThreadConsumer= new MultiThreadConsumer(zookeeper,groupId,topic);
		multiThreadConsumer.testMultiThreadConsumer(threadCount);
		//multiThreadConsumer.shutdown();
	}
	
	
}
