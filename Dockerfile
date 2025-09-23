# syntax=docker/dockerfile:1

FROM maven:3.9.11-eclipse-temurin-24 AS build
WORKDIR /workspace/app

COPY pom.xml ./
COPY mvnw ./
COPY .mvn .mvn
RUN chmod +x mvnw
RUN ./mvnw -B dependency:go-offline

COPY src ./src
RUN ./mvnw -B package -DskipTests

FROM eclipse-temurin:24-jre
WORKDIR /opt/can-cache
COPY --from=build /workspace/app/target/quarkus-app/ ./quarkus-app/

EXPOSE 11211
ENTRYPOINT ["java", "-jar", "/opt/can-cache/quarkus-app/quarkus-run.jar"]
