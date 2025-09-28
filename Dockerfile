# syntax=docker/dockerfile:1

FROM maven:3.9.11-eclipse-temurin-24 AS build
WORKDIR /workspace/app

COPY pom.xml ./
COPY mvnw ./
COPY .mvn .mvn
COPY can-cache-application/pom.xml can-cache-application/pom.xml
COPY can-cache-integration-tests/pom.xml can-cache-integration-tests/pom.xml
COPY can-cache-performance-tests/pom.xml can-cache-performance-tests/pom.xml
RUN chmod +x mvnw
RUN ./mvnw -B -pl can-cache-application -am dependency:go-offline

COPY can-cache-application/src can-cache-application/src
RUN ./mvnw -B -pl can-cache-application -am package -DskipTests

# Teşhis: target içeriğini ve varsa quarkus-app klasörünü listele
RUN ls -la can-cache-application/target && (ls -la can-cache-application/target/quarkus-app || true)

FROM eclipse-temurin:24-jre
WORKDIR /opt/can-cache
COPY --from=build /workspace/app/can-cache-application/target/quarkus-app ./quarkus-app
EXPOSE 11211 9000
ENTRYPOINT ["java","-jar","/opt/can-cache/quarkus-app/quarkus-run.jar"]
