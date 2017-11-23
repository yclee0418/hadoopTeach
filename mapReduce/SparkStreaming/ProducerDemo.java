package demo1.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) throws Exception {
		Properties pros = new Properties();
		pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		try (Producer<String, String> producer = new KafkaProducer<>(pros)) {
			for(int i = 0 + 10; i < 100 + 10; i++) {
				ProducerRecord<String, String> record = new ProducerRecord<>("mytopic1", Integer.toString(i),"The Message" + Integer.toString(i));
				Future<RecordMetadata> futureMetadata = producer.send(record);
				System.out.println("message of " + Integer.toString(i) + " sent!");
				Thread.sleep(500);
			}
		}
		System.out.println("message produce successfully!");
	}

}
