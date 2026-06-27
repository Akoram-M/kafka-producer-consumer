package com.kafka.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

// Implements Kafka's native Serializer interface
public class TransactionSerializer implements Serializer<Transaction> {
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No special configuration needed for this basic setup
    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        if (data == null) {
            return null;
        }
        try {
            // Converts the Java Object into a raw JSON byte array for network transit
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}