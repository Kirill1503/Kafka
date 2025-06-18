package com.example.shippings.messaging;

import com.example.shippings.DTO.Order;
import com.example.shippings.service.ShippingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ShippingConsumer {
    private final ObjectMapper objectMapper;
    private final ShippingService shippingService;
    private final Logger logger = LoggerFactory.getLogger(ShippingConsumer.class);
    private final ShippingProducer shippingProducer;

    public ShippingConsumer(ObjectMapper objectMapper, ShippingService shippingService, ShippingProducer shippingProducer) {
        this.objectMapper = objectMapper;
        this.shippingService = shippingService;
        this.shippingProducer = shippingProducer;
    }

    @KafkaListener(topics = "new_payment", groupId = "shippings-group")
    public void consume(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            logger.info("Received order {}", order);
            if (shippingService.shippingCreated(order)) {
                shippingProducer.sendMessage(order);
                logger.info("Shipping created");
            } else {
                logger.info("Shipping not created");
            }
        } catch (JsonProcessingException e) {
            logger.error("Shipping failed");
            throw new RuntimeException(e);
        }
    }
}
