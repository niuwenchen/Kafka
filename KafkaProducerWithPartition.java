package com.jackniu.test;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerWithPartition {
	private static Producer<String, String> producer;
	
	public KafkaProducerWithPartition()
	{
		Properties props =new Properties();
		/* multiple broker clusters
		 * props.put("metadata.broker.list", "192.168.222.128:9092 192.168.222.128:9093");
		 * */
		props.put("metadata.broker.list", "192.168.222.128:9092");
		/*This property specifies the serializer class that needs to be
		 * user while preparing the message for the transmission from the 
		 * producer to the broker.
		 */
		props.put("serializer.class", "kafka.serializer.StringEncoding");
		/*
		 * This property instructs the Kafka broker to send an acknowledgement
		 * to the producer when a message is received.
		 * the value 1 means the producer receives an acknowledgement once the lead replica
		 * has received the data
		 */
		props.put("request.required.acks", 1);
		props.put("partitioner.class", "com.jackniu.test.SimplePartitioner");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	
	public static void main(String[] args) {
		int argsCount = args.length;
		if(argsCount ==0 || argsCount ==1)
		{
			throw new IllegalArgumentException(
					"Please provide topic name and message count as arguments");
		}
		
		String topic=(String)args[0];
		String count=(String)args[1];
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - "+ topic);
		System.out.println("Message Count -"+ messageCount);
		KafkaProducerWithPartition kafkaProducerWithPartition= new KafkaProducerWithPartition();
		kafkaProducerWithPartition.publishMessage(topic, messageCount);
		
	}
	private void publishMessage(String topic,int messageCount)
	{	
		Random random = new Random();
		
		for(int mCount = 0; mCount<messageCount;mCount++)
		{
			String clientIP="192.169.14."+random.nextInt(255);
			String accessTime = new Date().toString();
			String message = accessTime+ ", kafka.apache.org,"+clientIP;
			System.out.println(message);
			
			KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,clientIP, message);
			producer.send(data);
		}
		producer.close();
	}
}
