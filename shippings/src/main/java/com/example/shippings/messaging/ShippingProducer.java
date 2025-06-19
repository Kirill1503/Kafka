package com.example.shippings.messaging;

import com.example.shippings.DTO.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value(value = "${app.kafka.shippings.topic}")
    private String shippingsTopic;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(ShippingProducer.class);

    public ShippingProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            kafkaTemplate.send(shippingsTopic, order.getId().toString(), json);
            logger.info("Shipping message sent to topic {}", shippingsTopic);
        } catch (JsonProcessingException e) {
            logger.error("Error while sending shipping message", e);
            throw new RuntimeException(e);
        }
    }
}
