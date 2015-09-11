package com.bonc.simple;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DpiProducer {
	
	public static void main(String[] args) throws Exception {
		
		BoncKafkaProducer producer = BoncKafkaProducer.getInstance(null);
		 
		InputStream fis =DpiProducer.class.getResourceAsStream("/kafka.properties");
        BufferedReader bis  = new BufferedReader(new InputStreamReader(fis,"UTF-8")); 
   
        String line = null;
        System.out.println("===========jobstarttime:"+new SimpleDateFormat("yyyy-MM–dd HH:mm:ss").format(new Date()));
		
		
        while ((line = bis.readLine()) != null) {
        	String[] values = line.split("\t", -1) ;
        	producer.send("douyy1", values[0], line);
        }
        
		
        producer.close();
        System.out.println("===========jobendtime:"+new SimpleDateFormat("yyyy-MM–dd HH:mm:ss").format(new Date()));
        System.exit(0);
    }
	
}
