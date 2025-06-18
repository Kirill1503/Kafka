package com.example.payments.messaging;

import com.example.payments.DTO.Order;
import com.example.payments.service.PaymentService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentsConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final Logger logger = LoggerFactory.getLogger(PaymentsConsumer.class);
    private final PaymentsProducer paymentsProducer;

    public PaymentsConsumer(ObjectMapper objectMapper, PaymentService paymentService, PaymentsProducer paymentsProducer) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
        this.paymentsProducer = paymentsProducer;
    }

    @KafkaListener(topics = "new_orders", groupId = "payments-group", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        try {
            Order order = objectMapper.readValue(message, Order.class);
            logger.info("Received order {}", order);
            if (paymentService.createPayment(order)) {
                paymentsProducer.sendMessage(order);
                logger.info("Payment created");
            } else {
                logger.warn("Payment not created for order {}", order);
                throw new RuntimeException("Payment creation failed");
            }
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize order message: {}", message, e);
            throw new RuntimeException(e);
        }
    }
}
