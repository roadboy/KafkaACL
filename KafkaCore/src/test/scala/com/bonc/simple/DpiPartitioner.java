package com.bonc.simple;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class DpiPartitioner implements Partitioner{
	
	public DpiPartitioner(VerifiableProperties props) {
	}
		 
	public int partition(Object key, int numPartitions) {
		// TODO Auto-generated method stub
		
		if (key == null) {
            Random random = new Random();
            return Math.abs(random.nextInt())%numPartitions;
        }
        else {
            int result = Math.abs(key.hashCode())%numPartitions; 
            return result;
        }
	}

}
