package com.trams.notification_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trams.common.dto.UserCreatedEvent;
import com.trams.notification_service.model.ProcessedEvent;
import com.trams.notification_service.repository.ProcessedEventRepository;
import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserCreatedConsumer {

    private final Connection natsConnection;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    @Value("${nats.stream-name}")
    private String streamName;

    @Value("${nats.subject}")
    private String subject;

    @Value("${nats.durable-consumer-name}")
    private String durableName;

    private JetStreamSubscription subscription;

    @PostConstruct
    public void startListening() {
        try {
            JetStream js = natsConnection.jetStream();

            // Durable Consumer: Retains stream progress across restarts
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(durableName)
                    .ackPolicy(AckPolicy.Explicit) // Explicit ACK required
                    .ackWait(Duration.ofSeconds(10))
                    .build();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .stream(streamName)
                    .configuration(consumerConfig)
                    .build();

            // Async message dispatcher
            Dispatcher dispatcher = natsConnection.createDispatcher();
            subscription = js.subscribe(subject, dispatcher, this::handleMessage, false, options);
            log.info("Subscribed to subject [{}] with durable consumer [{}]", subject, durableName);

        } catch (Exception e) {
            log.error("Failed to bind durable NATS JetStream consumer: {}", e.getMessage());
        }
    }

    private void handleMessage(Message msg) {
        try {
            String json = new String(msg.getData(), StandardCharsets.UTF_8);
            UserCreatedEvent event = objectMapper.readValue(json, UserCreatedEvent.class);

            // 1. Idempotency Check (Persistent Database Table)
            if (processedEventRepository.existsById(event.eventId())) {
                log.warn("[DUPLICATE EVENT DETECTED] Event ID [{}] already processed. Skipping dispatch.", event.eventId());
                msg.ack();
                return;
            }

            // 2. Simulate Business Action (Notification Dispatch)
            sendNotification(event);

            // 3. Mark Event as Processed
            ProcessedEvent processed = new ProcessedEvent(
                    event.eventId(),
                    "USER_CREATED",
                    Instant.now()
            );
            processedEventRepository.save(processed);

            // 4. Acknowledge to JetStream
            msg.ack();
            log.info("[ACKNOWLEDGED] Event [{}] committed successfully", event.eventId());

        } catch (Exception e) {
            log.error("Error processing event: {}. Triggering NAK.", e.getMessage());
            msg.nak(); // NAK triggers redelivery from JetStream
        }
    }

    private void sendNotification(UserCreatedEvent event) {
        log.info("------------------------------------------------------------");
        log.info("DISPATCHING NOTIFICATION:");
        log.info("  Recipient : {} <{}>", event.fullName(), event.email());
        log.info("  User ID   : {}", event.userId());
        log.info("  Message   : Welcome to Trams, {}! Your registration is complete.", event.fullName());
        log.info("  Event ID  : {}", event.eventId());
        log.info("------------------------------------------------------------");
    }
}