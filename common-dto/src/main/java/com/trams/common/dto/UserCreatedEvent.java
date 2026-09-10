package com.trams.common.dto;

import java.io.Serializable;
import java.time.Instant;

/**
 * Event published when a user is successfully registered.
 * Implements Serializable for byte-stream messaging over NATS.
 */
public record UserCreatedEvent(
        String eventId,     // Unique UUID for Idempotency (prevent duplicate notification sends)
        Long userId,        // Database ID
        String fullName,    // User's name
        String email,       // Recipient email
        Instant createdAt   // Event creation timestamp (UTC)
) implements Serializable {}