package ru.checkdev.notification.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.checkdev.notification.dto.WisherApprovedDTO;
import ru.checkdev.notification.service.NotificationMessagesService;

@AllArgsConstructor
@Component
public class NotificationWisherConsumer {

    private final NotificationMessagesService notificationMessagesService;

    @KafkaListener(topics = "notification-wisher-send-message-approve")
    public void sendMessageApprovedWisher(WisherApprovedDTO wisherApprovedNotifyDTO) {
        notificationMessagesService.sendApprovedNotification(wisherApprovedNotifyDTO);
    }
}
