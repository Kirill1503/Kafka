package com.example.payments.messaging;

import com.example.payments.DTO.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentsProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value(value = "${app.kafka.payments.topic}")
    private String paymentsTopic;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(PaymentsProducer.class);

    public PaymentsProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            kafkaTemplate.send(paymentsTopic, order.getId().toString(), json);
            logger.info("Payment message sent to topic {}", paymentsTopic);
        } catch (JsonProcessingException e) {
            logger.error("Error while sending payment message", e);
            throw new RuntimeException(e);
        }
    }
}