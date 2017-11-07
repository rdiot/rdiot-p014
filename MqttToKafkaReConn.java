/* ARTIK MQTT+Raspberry Pi Apache Kafka Cluster Bridge [P014] : http://rdiot.tistory.com/336 [RDIoT Demo] */

package com.rdiot.mqtt_kafka_bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.fusesource.mqtt.client.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;



public class MqttToKafkaReConn {
	
	//MQTT Client - MQTT Consumer
	private static final String MQTT_SERVER_HOST = "192.168.0.106";
	private static final int MQTT_SERVER_PORT = 1883;
	private static final String MQTT_SERVER_TOPICS = "temperature,humidity,cds,airGrade";
	
	//Kafka Producer 
	private static final String KAFKA_BROKER_LIST = "kafka-pi-01:9092,kafka-pi-02:9092,kafka-pi-03:9092";
	private static final int KAFKA_RECONN_CNT = 1000;	 	
	private static final int MQTT_RECONN_CNT = 1000;  // must be under 279621
	static byte[] qoses;

	
	public static void main(String[] args) throws Exception {
		
		MQTT mqtt = new MQTT();		
		mqtt.setHost(MQTT_SERVER_HOST, MQTT_SERVER_PORT);
		BlockingConnection bconn = null;		
				
		
		List<Topic> topicList = new ArrayList<Topic>();
		String[] topic = MQTT_SERVER_TOPICS.split(",");
		int topicCount = 0;
		
		for(String addtopic:topic) {
			topicList.add(new Topic(addtopic, QoS.EXACTLY_ONCE));
			topicCount++;
		}

		Topic[] subscribeTopics = topicList.toArray(new Topic[]{});
				
		
		Properties props = new Properties();
		props.put("metadata.broker.list", KAFKA_BROKER_LIST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");		
		props.put("block.on.buffer.full", "false");

		ProducerConfig kafkaProducerConfig = new ProducerConfig(props);
		
		boolean kafkaReconn = true;
		int num = 0;
		Producer<String, String> kafkaProducer = null;
		
		boolean mqttReconn = true;
		
		
		while(true) {
			
			if(mqttReconn) {
				bconn = mqtt.blockingConnection();
				bconn.connect();
				qoses = bconn.subscribe(subscribeTopics);
				mqttReconn = false;	
				System.out.println("mqtt connected");
				
			}
			
			if(kafkaReconn) {
				kafkaProducer = new Producer<String, String>(kafkaProducerConfig);
				kafkaReconn = false;
				System.out.println("kafka connected");
			}
			
			String strRtn = "";
			long time = System.currentTimeMillis();
			strRtn += time;

			num++;
			Message msg = bconn.receive();
			byte[] pl = msg.getPayload();
			String strPl = new String(pl);			
			String topicName = msg.getTopic();
			
			System.out.print("["+num+"]");
			System.out.print(topicName+"="+strPl+"\t");		
			
			strRtn += " ";
			strRtn += strPl;
			
			KeyedMessage<String, String> kafkaMsg = new KeyedMessage<String, String>(topicName, strRtn);
			kafkaProducer.send(kafkaMsg);
			
						
			if(num%topicCount  == 0) {
				System.out.println();
			}
			
			// mqtt reconnect 
			if(num % MQTT_RECONN_CNT == 0) {				
				bconn.disconnect();
				mqttReconn = true;				
			}
			
			// kafka recoonect
			if(num % KAFKA_RECONN_CNT == 0) {				
				kafkaProducer.close();
				kafkaReconn = true;				
			}
						
		}
		
	}
}
