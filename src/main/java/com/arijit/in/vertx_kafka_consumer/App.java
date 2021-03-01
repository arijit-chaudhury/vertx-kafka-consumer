package com.arijit.in.vertx_kafka_consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;

/**
 * Hello world!
 *
 */
public class App extends AbstractVerticle {

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

		TopicPartition tp = new TopicPartition().setPartition(0).setTopic("employees-topic");
		consumer.assign(tp, ar -> {
			if (ar.succeeded()) {
				System.out.println("Subscribed");
				consumer.assignment(done1 -> {
					if (done1.succeeded()) {
						for (TopicPartition topicPartition : done1.result()) {
							System.out.println("Partition: topic={}, number={}"+": "+topicPartition.getTopic()+": "+topicPartition.getPartition());
						}
					} else {
						System.out.println("Could not assign partition: err={}"+": "+done1.cause().getMessage());
					}
				});
			} else {
				System.out.println("Could not subscribe: err= " + ar.cause().getMessage());
			}
		});
		consumer.handler(record -> {
			System.out.println("Processing: key={}, value={}, partition={}, offset={}"+": "+record.key()+": "+record.value()+": "+record.partition()+": "+record.offset());
		});
	}

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(new App());
	}
}
