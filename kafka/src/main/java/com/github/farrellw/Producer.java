package com.github.farrellw;

import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO adjust producer to produce records to the reviews topic ( Need to find the review data structure )
public class Producer {

    public static void main(String[] args) throws JsonProcessingException {

        Dotenv dotenv = Dotenv.load();
        String gcpBootstrapServer = dotenv.get("BOOTSTRAP_SERVER");
        String topic = "orders";

        Logger logger = LoggerFactory.getLogger(Producer.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, gcpBootstrapServer);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ObjectMapper objectMapper = new ObjectMapper();
        Faker faker = new Faker();

        for (int i = 0; i < 10; i++) {
            Order order = new Order(faker.commerce().price(), faker.commerce().productName());
            String customerId = Integer.toString(i);
            String jsonString = objectMapper.writeValueAsString(order);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, customerId, jsonString);

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
//                            logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n"
//                                    + "Partition:" + recordMetadata.partition() + "\n" + "Offset: "
//                                    + recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });

        }

        producer.flush();

        // flush and close
        producer.close();
        System.exit(0);
    }
}