package ru.checkdev.notification.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.checkdev.notification.dto.FeedbackNotificationDTO;
import ru.checkdev.notification.service.NotificationMessagesService;

@AllArgsConstructor
@Component
public class FeedbackNotificationConsumer {

    private final NotificationMessagesService notificationMessagesService;

    @KafkaListener(topics = "feedback-notification")
    public void sendFeedbackNotification(FeedbackNotificationDTO feedbackNotification) {
        notificationMessagesService.sendFeedbackNotification(feedbackNotification);
    }
}
