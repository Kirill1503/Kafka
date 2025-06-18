package orders.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import orders.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value(value = "${app.kafka.orders.topic}")
    private String topic;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            kafkaTemplate.send(topic, json);
            logger.info("Message sent to topic {}", topic);
        } catch (JsonProcessingException e) {
            logger.error("Error while sending order message", e);
            throw new RuntimeException(e);
        }
    }
}
