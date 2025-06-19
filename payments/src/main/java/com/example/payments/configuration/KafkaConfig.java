package com.example.payments.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value(value = "${app.kafka.payments.topic}")
    private String paymentsTopic;
    @Value(value = "${app.kafka.topic.partitions}")
    private Integer countPartitions;

    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name(paymentsTopic)
                .partitions(countPartitions)
                .replicas(1)
                .build();
    }
}
