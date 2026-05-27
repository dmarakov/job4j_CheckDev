package ru.checkdev.notification.kafka;


import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import ru.checkdev.notification.domain.InnerMessage;
import ru.checkdev.notification.domain.UserTelegram;
import ru.checkdev.notification.dto.CancelInterviewNotificationDTO;
import ru.checkdev.notification.dto.InterviewNotifyDTO;
import ru.checkdev.notification.dto.WisherDismissedDTO;
import ru.checkdev.notification.dto.WisherNotifyDTO;
import ru.checkdev.notification.service.InnerMessageService;
import ru.checkdev.notification.service.MessagesGenerator;
import ru.checkdev.notification.service.NotificationMessage;
import ru.checkdev.notification.service.UserTelegramService;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
@Component
public class NotificationInterviewConsumer {

    private final UserTelegramService userTelegramService;
    private final InnerMessageService innerMessageService;
    private final NotificationMessage<UserTelegram, String, InnerMessage> notificationMessage;
    private final MessagesGenerator messagesGenerator;

    @KafkaListener(topics = "notification-interview-send-message-subscribe")
    public void sendMessageSubscribeTopic(InterviewNotifyDTO interviewNotifyDTO) {
        List<UserTelegram> usersTopic = userTelegramService
            .findAllByTopicIdAndUserIdNot(interviewNotifyDTO.getTopicId(),
                interviewNotifyDTO.getSubmitterId());
        var message = messagesGenerator.getMessageSubscribeTopic(interviewNotifyDTO);
        notificationMessage.sendMessage(usersTopic, message);
    }

    @KafkaListener(topics = "notification-interview-send-message-submitter")
    public void sendMessageSubmitterInterview(WisherNotifyDTO wisherNotifyDTO) {
        var message = messagesGenerator.getMessageParticipateWisher(wisherNotifyDTO);
        InnerMessage innerMessage = InnerMessage.of()
            .userId(wisherNotifyDTO.getSubmitterId())
            .text(message)
            .created(Timestamp.valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)))
            .read(false)
            .interviewId(wisherNotifyDTO.getInterviewId())
            .build();
        innerMessageService.saveMessage(innerMessage);
        userTelegramService
            .findByUserId(wisherNotifyDTO.getSubmitterId())
            .ifPresent(
                tg -> notificationMessage.sendMessage(tg, message)
            );
    }

    @KafkaListener(topics = "notification-interview-send-message-cancel")
    public void sendMessageCancelInterview(@RequestBody CancelInterviewNotificationDTO cancelInterviewDTO) {
        var message = messagesGenerator.getMessageCancelInterview(cancelInterviewDTO);
        InnerMessage innerMessage = InnerMessage.of()
            .userId(cancelInterviewDTO.getUserId())
            .text(message)
            .created(Timestamp.valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)))
            .read(false)
            .interviewId(cancelInterviewDTO.getInterviewId())
            .build();
        CompletableFuture.supplyAsync(() -> innerMessageService.saveMessage(innerMessage));
        userTelegramService
            .findByUserId(cancelInterviewDTO.getUserId())
            .ifPresent(
                tg -> notificationMessage.sendMessage(tg, message)
            );
    }

    @KafkaListener(topics = "notification-interview-send-message-dismissed")
    public void sendMessageCancelInterview(@RequestBody List<WisherDismissedDTO> wisherDtoList) {
        List<InnerMessage> innerMessageList = new ArrayList<>();
        wisherDtoList.parallelStream().forEach(wisher -> {
                var message = messagesGenerator.getMessageDismissedWisher(wisher);
                InnerMessage innerMessage = InnerMessage.of()
                    .userId(wisher.getUserId())
                    .text(message)
                    .created(Timestamp.valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)))
                    .read(false)
                    .interviewId(wisher.getInterviewId())
                    .build();
                CompletableFuture.supplyAsync(() -> innerMessageService.saveMessage(innerMessage));
                userTelegramService
                    .findByUserId(wisher.getUserId())
                    .ifPresent(
                        tg -> notificationMessage.sendMessage(tg, message)
                    );
            }
        );
    }
}
