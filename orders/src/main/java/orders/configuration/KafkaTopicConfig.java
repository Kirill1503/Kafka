package orders.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Value(value = "${app.kafka.orders.topic}")
    private String ordersTopic;
    @Value(value = "${app.kafka.topic.partitions}")
    private Integer countPartitions;

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(countPartitions)
                .replicas(1)
                .build();
    }
}
