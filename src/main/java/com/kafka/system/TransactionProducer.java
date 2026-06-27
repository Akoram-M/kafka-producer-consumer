package com.kafka.system;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
//docker exec -it local-kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-payments --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
public class TransactionProducer {
    
    private static final String TOPIC = "raw-payments";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // 1. Configure the Producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // Keys are strings (e.g., userId)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Values are our custom Transaction objects, handled by our custom Serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());
        
        // 2. Instantiate the Producer (Establish connection to Broker)
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        System.out.println("Starting Transaction Producer...");

        try {
            // Generate 100 mock transactions
            for (int i = 0; i < 100; i++) {
                String txId = UUID.randomUUID().toString();
                String userId = "USER_" + (int)(Math.random() * 10); // Generates USER_0 to USER_9
                double amount = Math.round((Math.random() * 1000) * 100.0) / 100.0;
                
                Transaction transaction = new Transaction(txId, userId, amount, "MERCHANT_A", System.currentTimeMillis());

                // 3. Create the Record. 
                // Notice we set the KEY to the userId. This is critical for Phase 2 (Partitioning).
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC, userId, transaction);

                // 4. Send the data asynchronously
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent: %s to partition: %d, offset: %d%n", 
                                transaction.getTransactionId(), metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("Failed to send record: " + exception.getMessage());
                    }
                });

                // Throttle generation to simulate real-world traffic
                Thread.sleep(500); 
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 5. Always flush and close to prevent data loss in the internal buffer
            producer.flush();
            producer.close();
            System.out.println("Producer closed.");
        }
    }
}