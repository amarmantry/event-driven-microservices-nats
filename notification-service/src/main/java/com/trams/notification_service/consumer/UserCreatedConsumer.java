package com.trams.notification_service.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trams.common.dto.UserCreatedEvent;
import com.trams.notification_service.model.ProcessedEvent;
import com.trams.notification_service.repository.ProcessedEventRepository;
import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserCreatedConsumer {

    private final Connection natsConnection;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    private static final String STREAM_NAME = "USERS_STREAM";
    private static final String SUBJECT = "user.events.created";
    private static final String CONSUMER_NAME = "notification-service-consumer";

    @PostConstruct
    public void startConsumer() {
        try {
            JetStreamManagement jsm = natsConnection.jetStreamManagement();

            boolean streamExists = false;
            try {
                StreamInfo info = jsm.getStreamInfo(STREAM_NAME);
                streamExists = (info != null);
            } catch (JetStreamApiException e) {
                if (e.getErrorCode() == 10059) {
                    streamExists = false;
                }
            }

            if (!streamExists) {
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name(STREAM_NAME)
                        .subjects(SUBJECT)
                        .storageType(StorageType.File)
                        .build();
                jsm.addStream(streamConfig);
                log.info("USERS_STREAM auto-created by notification-service");
            }

            JetStream js = natsConnection.jetStream();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable(CONSUMER_NAME)
                    .build();

            Dispatcher dispatcher = natsConnection.createDispatcher();

            js.subscribe(SUBJECT, dispatcher, (Message msg) -> {
                String json = new String(msg.getData(), StandardCharsets.UTF_8);
                UserCreatedEvent event;

                // 1. Poison-pill protection: discard unparseable payloads immediately
                try {
                    event = objectMapper.readValue(json, UserCreatedEvent.class);
                } catch (JsonProcessingException e) {
                    log.error("[POISON PILL] Failed to deserialize JSON. Discarding unrecoverable message: {}", json, e);
                    msg.ack();
                    return;
                }

                // 2. Business logic & idempotency barrier
                try {
                    if (processedEventRepository.existsById(event.eventId())) {
                        log.warn("[DUPLICATE DETECTED] Event [{}] already processed. Discarding.", event.eventId());
                        msg.ack();
                        return;
                    }

                    log.info("------------------------------------------------------------");
                    log.info("DISPATCHING NOTIFICATION:");
                    log.info("  Recipient : {} <{}>", event.fullName(), event.email());
                    log.info("  User ID   : {}", event.userId());
                    log.info("  Message   : Welcome to Trams, {}! Your registration is complete.", event.fullName());
                    log.info("  Event ID  : {}", event.eventId());
                    log.info("------------------------------------------------------------");

                    processedEventRepository.save(new ProcessedEvent(event.eventId(), "USER_CREATED"));

                    msg.ack();
                    log.info("[ACKNOWLEDGED] Event [{}] committed successfully", event.eventId());

                } catch (Exception e) {
                    log.error("Transient error processing event [{}]. NAK-ing for redelivery", event.eventId(), e);
                    msg.nak();
                }
            }, false, options);

            log.info("Notification consumer bound successfully to [{}] on subject [{}]", STREAM_NAME, SUBJECT);

        } catch (Exception e) {
            log.error("Failed to bind durable NATS JetStream consumer: {}", e.getMessage(), e);
        }
    }
}