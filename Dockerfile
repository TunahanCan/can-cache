# syntax=docker/dockerfile:1

FROM maven:3.9.11-eclipse-temurin-24 AS build
WORKDIR /workspace/app

COPY pom.xml ./
COPY mvnw ./
COPY .mvn .mvn
COPY application/pom.xml application/pom.xml
COPY integration-tests/pom.xml integration-tests/pom.xml
COPY performance-tests/pom.xml performance-tests/pom.xml
COPY performance-tests/java-sampler/pom.xml performance-tests/java-sampler/pom.xml
RUN chmod +x mvnw
RUN ./mvnw -B -pl application -am dependency:go-offline

COPY application/src application/src
RUN ./mvnw -B -pl application -am package -DskipTests

# Teşhis: target içeriğini ve varsa quarkus-app klasörünü listele
RUN ls -la application/target && (ls -la application/target/quarkus-app || true)

FROM eclipse-temurin:24-jre
WORKDIR /opt/can-cache
COPY --from=build /workspace/app/application/target/*-runner.jar ./app.jar
EXPOSE 11211
ENTRYPOINT ["java","-jar","/opt/can-cache/app.jar"]
