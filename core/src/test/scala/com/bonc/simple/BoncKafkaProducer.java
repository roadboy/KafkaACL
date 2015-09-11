package com.bonc.simple;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class BoncKafkaProducer {
	
	private static Map<String,BoncKafkaProducer> map = new HashMap<String,BoncKafkaProducer>();
	private static Producer<String,String> producer ;
	
	static{
		BoncKafkaProducer dpiProducer = new BoncKafkaProducer();
		map.put(producer.getClass().getName(), dpiProducer);
	}
	//保护的默认构造子
	protected BoncKafkaProducer(){
		InputStream fis ; 
		fis =BoncKafkaProducer.class.getResourceAsStream("/kafka.properties");
		Properties props = new Properties();
		try {
			props.load(fis);
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}            
        ProducerConfig config = new ProducerConfig(props);
        producer=new Producer<String, String>(config);
	}
	
	public static BoncKafkaProducer getInstance(String name) {
		if(name == null) {
			name = BoncKafkaProducer.class.getName();
		}
		if(map.get(name) == null) {
			try {
				map.put(name, (BoncKafkaProducer) Class.forName(name).newInstance());
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
		    }
		 }
		    return map.get(name);
	}
	
	
	public void send(String topic,String key,String value) throws Exception{
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key,value); 
        producer.send(data);    
	}
	
	public void close(){
		producer.close();
	}
	
}
