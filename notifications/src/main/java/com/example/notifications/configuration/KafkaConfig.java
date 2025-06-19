package com.example.notifications.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value(value = "${app.kafka.notifications.topic}")
    private String notificationsTopic;
    @Value(value = "${app.kafka.topic.partitions}")
    private Integer countPartitions;

    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name(notificationsTopic)
                .partitions(countPartitions)
                .replicas(1)
                .build();
    }
}
