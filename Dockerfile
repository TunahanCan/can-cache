# syntax=docker/dockerfile:1

FROM gradle:9.5.1-jdk25 AS build
USER root
WORKDIR /workspace/app

COPY build.gradle settings.gradle gradle.properties gradlew gradlew.bat ./
COPY gradle gradle
COPY can-cache-application/build.gradle can-cache-application/build.gradle
COPY can-cache-integration-tests/build.gradle can-cache-integration-tests/build.gradle
COPY can-cache-performance-tests/build.gradle can-cache-performance-tests/build.gradle
COPY can-cache-agent/build.gradle can-cache-agent/build.gradle

RUN chmod +x gradlew
RUN ./gradlew --no-daemon :can-cache-application:quarkusGoOffline

COPY can-cache-application/src can-cache-application/src
RUN ./gradlew --no-daemon :can-cache-application:build -x test

FROM eclipse-temurin:25-jre
WORKDIR /opt/can-cache
COPY --from=build /workspace/app/can-cache-application/build/quarkus-app ./quarkus-app
EXPOSE 11211
ENTRYPOINT ["java","-jar","/opt/can-cache/quarkus-app/quarkus-run.jar"]
