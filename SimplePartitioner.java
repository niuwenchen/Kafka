package com.jackniu.test;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
	public SimplePartitioner(VerifiableProperties props)
	{
	}

	public int partition(Object key, int numPartitioner) {
		int partition =0;
		String partitionKey = (String) key;
		int offset = partitionKey.lastIndexOf('.');
		if(offset > 0)
		{
			partition=Integer.parseInt(partitionKey.substring(offset+1))%numPartitioner;
			
		}
		return partition;
	}	
}