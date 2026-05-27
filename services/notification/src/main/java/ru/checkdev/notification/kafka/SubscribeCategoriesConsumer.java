package ru.checkdev.notification.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.checkdev.notification.domain.SubscribeCategory;
import ru.checkdev.notification.service.SubscribeCategoryService;

@AllArgsConstructor
@Component
public class SubscribeCategoriesConsumer {
    private final SubscribeCategoryService service;

    @KafkaListener(topics = "subscribe-categories-add")
    public void toAddSubscribeCategory(SubscribeCategory subscribeCategory
    )
    {
        service.save(subscribeCategory);
    }

    @KafkaListener(topics = "subscribe-categories-delete")
    public void toDeleteSubscribeCategory(SubscribeCategory subscribeCategory
    )
    {
        service.delete(subscribeCategory);
    }
}
