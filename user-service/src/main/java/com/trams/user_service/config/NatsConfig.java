package com.trams.user_service.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.Duration;

@Slf4j
@Configuration
public class NatsConfig {

    @Value("${nats.url}")
    private String natsUrl;

    @Value("${nats.token}")
    private String natsToken;

    @Value("${nats.stream-name}")
    private String streamName;

    @Value("${nats.subject}")
    private String subject;

    // --- ADD THIS BEAN ---
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean
    public Connection natsConnection() throws IOException, InterruptedException {
        Options options = new Options.Builder()
                .server(natsUrl)
                .token(natsToken.toCharArray())
                .connectionTimeout(Duration.ofSeconds(5))
                .build();

        Connection connection = Nats.connect(options);
        log.info("Connected to NATS broker at {}", natsUrl);

        provisionJetStreamStream(connection);

        return connection;
    }

    @Bean
    public JetStream jetStream(Connection connection) throws IOException {
        return connection.jetStream();
    }

    private void provisionJetStreamStream(Connection connection) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            StreamInfo streamInfo = null;
            try {
                streamInfo = jsm.getStreamInfo(streamName);
            } catch (JetStreamApiException e) {
                // Stream does not exist yet
            }

            if (streamInfo == null) {
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subject)
                        .storageType(StorageType.File)
                        .build();
                jsm.addStream(streamConfig);
                log.info("Created JetStream Stream [{}] bound to subject [{}]", streamName, subject);
            } else {
                log.info("JetStream Stream [{}] already exists", streamName);
            }
        } catch (Exception e) {
            log.error("Error provisioning JetStream stream: {}", e.getMessage());
        }
    }
}