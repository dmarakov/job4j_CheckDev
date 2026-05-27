package ru.checkdev.notification.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.checkdev.notification.domain.SubscribeTopic;
import ru.checkdev.notification.service.SubscribeTopicService;

@AllArgsConstructor
@Component
public class SubscribeTopicConsumer {

    private final SubscribeTopicService service;

    @KafkaListener(topics = "subscribe-topic-add")
    public void toAddSubscribeTopic(SubscribeTopic subscribeTopic) {
        service.save(subscribeTopic);
    }

    @KafkaListener(topics = "subscribe-topic-delete")
    public void toDeleteSubscribeTopic(SubscribeTopic subscribeTopic) {
        service.delete(subscribeTopic);
    }
}
