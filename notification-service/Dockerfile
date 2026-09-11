FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build

COPY common-dto/ ./common-dto/
RUN cd common-dto && mvn clean install -DskipTests

COPY notification-service/ ./notification-service/
RUN cd notification-service && mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/notification-service/target/*.jar app.jar

EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]