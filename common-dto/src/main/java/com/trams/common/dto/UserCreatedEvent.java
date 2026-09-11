package com.trams.common.dto;

import java.io.Serializable;
import java.time.Instant;

public record UserCreatedEvent(
        String eventId,      // UUID for idempotency
        Long userId,
        String fullName,
        String email,
        Instant createdAt
) implements Serializable {}