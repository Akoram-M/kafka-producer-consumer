package com.kafka.system;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
//mvn exec:java -Dexec.mainClass="com.system.kafka.LoggerConsumer"
public class LoggerConsumer {

    private static final String TOPIC = "raw-payments";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "logger-group-3";

    public static void main(String[] args) {
        // 1. Configure the Consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        
        // Instruct consumer to read from the beginning if it has no previous offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Set Deserializers to reconstruct strings and our custom Transaction objects
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());

        // 2. Instantiate the Consumer
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        // 3. Subscribe to the topic
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Starting Logger Consumer. Waiting for data...");

        try {
            // 4. The Infinite Polling Loop
            // Consumers are active. They must continually ask the broker for data.
            while (true) {
                // Poll blocks for up to 1000ms waiting for data
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Transaction> record : records) {
                    System.out.printf("Consumed Record - Key: %s, Value: %s, Partition: %d, Offset: %d%n",
                            record.key(), record.value().toString(), record.partition(), record.offset());
                }
            }
        } finally {
            // Safely leave the consumer group
            consumer.close();
        }
    }
}