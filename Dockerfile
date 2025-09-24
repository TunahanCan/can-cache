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

# Teşhis: target içeriğini ve varsa quarkus-app klasörünü listele
RUN ls -la target && (ls -la target/quarkus-app || true)

FROM eclipse-temurin:24-jre
WORKDIR /opt/can-cache
COPY --from=build /workspace/app/target/*-runner.jar ./app.jar
EXPOSE 11211
ENTRYPOINT ["java","-jar","/opt/can-cache/app.jar"]