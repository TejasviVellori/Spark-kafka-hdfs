package kafkaproducer;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class StringGenerator 
{
	public static void main(String[] args) 
	{
      
        Random r = new Random();
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.0.0.17:9092,10.0.0.18:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "main.java.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        String[] content      = {"Message from MqttPublishSample","message is delivered to mqtt broker","mqtt broker is connected"};
       for(;;){
               String msg = content[r.nextInt(content.length)] ; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("stringdata",  msg);
               producer.send(data);
               try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
       }
    }
}
