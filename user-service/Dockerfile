FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build

COPY common-dto/ ./common-dto/
RUN cd common-dto && mvn clean install -DskipTests

COPY user-service/ ./user-service/
RUN cd user-service && mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/user-service/target/*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]