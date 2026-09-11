package com.trams.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ApiGatewayApplication {

    private static final Logger log = LoggerFactory.getLogger(ApiGatewayApplication.class);

    @Value("${user-service.url:http://localhost:8081}")
    private String userServiceUrl;

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder) {
        log.info(">>> Registering Gateway route for /api/v1/users/** -> {}", userServiceUrl);
        return builder.routes()
                .route("user-service-route", r -> r
                        .path("/api/v1/users/**")
                        .filters(f -> f.addRequestHeader("X-Gateway-Processed", "true"))
                        .uri(userServiceUrl)
                )
                .build();
    }
}