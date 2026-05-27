package ru.checkdev.notification.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.checkdev.notification.domain.InnerMessage;
import ru.checkdev.notification.dto.CategoryWithTopicDTO;
import ru.checkdev.notification.service.InnerMessageService;
import ru.checkdev.notification.service.NotificationMessagesService;
import ru.checkdev.notification.service.SubscribeCategoryService;
import ru.checkdev.notification.service.SubscribeTopicService;

import java.util.List;

@AllArgsConstructor
@Component
public class InnerMessageConsumer {

    private final InnerMessageService messageService;
    private final SubscribeCategoryService categoryService;
    private final SubscribeTopicService topicService;
    private final NotificationMessagesService notificationMessagesService;

    @KafkaListener(topics = "inner-messages-create-message")
    public void createMessage(CategoryWithTopicDTO categoryWithTopicDTO) {

        List<Integer> categorySubscribersIds =
            categoryService.findUserIdsByCategoryIdExcludeCurrent(
                categoryWithTopicDTO.getCategoryId(),
                categoryWithTopicDTO.getSubmitterId());

        List<Integer> topicSubscribersIds =
            topicService.findUserIdsByTopicIdExcludeCurrent(
                categoryWithTopicDTO.getTopicId(),
                categoryWithTopicDTO.getSubmitterId());

        messageService.saveMessagesForSubscribers(
            categoryWithTopicDTO,
            categorySubscribersIds, topicSubscribersIds);

        notificationMessagesService.sendMessagesToCategorySubscribers(
            categorySubscribersIds,
            categoryWithTopicDTO);
    }

    @KafkaListener(topics = "inner-messages-send-message")
    public void sendMessage(InnerMessage innerMessage) {
        messageService.send(innerMessage);
    }
}
