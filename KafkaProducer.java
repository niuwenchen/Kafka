package com.jackniu.test;

import java.util.Date;
import java.util.Properties;

import javax.management.loading.PrivateClassLoader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private static Producer<String, String> producer;
	public KafkaProducer(){
		Properties props =new Properties();
		/* multiple broker clusters
		 * props.put("metadata.broker.list", "192.168.222.128:9092 192.168.222.128:9093");
		 * */
		props.put("metadata.broker.list", "192.168.222.128:9092");
		/*This property specifies the serializer class that needs to be
		 * user while preparing the message for the transmission from the 
		 * producer to the broker.
		 */
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		/*
		 * This property instructs the Kafka broker to send an acknowledgement
		 * to the producer when a message is received.
		 * the value 1 means the producer receives an acknowledgement once the lead replica
		 * has received the data
		 */
		props.put("request.required.acks", "1");
		
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
		
		KafkaProducer kafkaProducer=new KafkaProducer();
		kafkaProducer.publishMessage(topic, messageCount);
		
	}
	private void publishMessage(String topic,int messageCount)
	{	
		for(int mCount = 0; mCount<messageCount;mCount++)
		{
			String runtime = new Date().toString();
			String msg = "Message Publishing Time - "+ runtime;
			System.out.println(msg);
			
			KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic, msg);
			producer.send(data);
		}
		producer.close();
	}

}
