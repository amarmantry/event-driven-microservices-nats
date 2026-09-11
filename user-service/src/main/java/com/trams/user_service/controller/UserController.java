package com.trams.user_service.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trams.common.dto.UserCreatedEvent;
import com.trams.user_service.dto.RegisterUserRequest;
import com.trams.user_service.model.User;
import com.trams.user_service.repository.UserRepository;
import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {

    private final UserRepository userRepository;
    private final JetStream jetStream;
    private final ObjectMapper objectMapper;

    @Value("${nats.subject}")
    private String subject;

    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterUserRequest request) {
        // Validation check for duplicates
        if (userRepository.findByEmail(request.email()).isPresent()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Email already registered"));
        }

        // 1. Persist User in DB
        User user = User.builder()
                .fullName(request.fullName())
                .email(request.email())
                .createdAt(Instant.now())
                .build();
        User savedUser = userRepository.save(user);

        // 2. Build Event with unique UUID for Idempotency
        String eventId = UUID.randomUUID().toString();
        UserCreatedEvent event = new UserCreatedEvent(
                eventId,
                savedUser.getId(),
                savedUser.getFullName(),
                savedUser.getEmail(),
                savedUser.getCreatedAt()
        );

        // 3. Publish Event asynchronously to NATS JetStream
        try {
            byte[] payload = objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8);
            PublishAck ack = jetStream.publish(subject, payload);
            log.info("Published UserCreatedEvent [id: {}] to stream [{}] seq [{}]",
                    eventId, ack.getStream(), ack.getSeqno());
        } catch (Exception e) {
            log.error("Failed to publish event to NATS broker: {}", e.getMessage(), e);

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of(
                            "error", "User was created, but the registration event could not be published"
                    ));
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(savedUser);
    }
}