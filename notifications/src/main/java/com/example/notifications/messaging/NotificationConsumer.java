package com.example.notifications.messaging;

import com.example.notifications.DTO.Order;
import com.example.notifications.service.NotificationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final Logger logger = LoggerFactory.getLogger(NotificationConsumer.class);

    public NotificationConsumer(ObjectMapper objectMapper, NotificationService notificationService) {
        this.objectMapper = objectMapper;
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "new_shipping", groupId = "notifications-group")
    public void consume(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            logger.info("Received Order Details: {}", order);
            String result = notificationService.sendNotification(order);
            logger.info("Notification result: {}", result);
        } catch (JsonProcessingException e) {
            logger.error("Error while processing Order Details: {}", message, e);
        }
    }
}
