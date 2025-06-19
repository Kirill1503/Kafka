package com.example.shippings.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value(value = "${app.kafka.shippings.topic}")
    private String shippingsTopic;
    @Value(value = "${app.kafka.topic.partitions}")
    private Integer countPartitions;

    @Bean
    public NewTopic shippingsTopic() {
        return TopicBuilder.name(shippingsTopic)
                .partitions(countPartitions)
                .replicas(1)
                .build();
    }
}
