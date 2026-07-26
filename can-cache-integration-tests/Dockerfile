FROM gradle:9.5.1-jdk25

USER root
WORKDIR /workspace

COPY build.gradle settings.gradle gradle.properties gradlew gradlew.bat ./
COPY gradle gradle
COPY can-cache-application/build.gradle can-cache-application/build.gradle
COPY can-cache-integration-tests/build.gradle can-cache-integration-tests/build.gradle
COPY can-cache-performance-tests/build.gradle can-cache-performance-tests/build.gradle
COPY can-cache-agent/build.gradle can-cache-agent/build.gradle

RUN chmod +x gradlew
RUN ./gradlew --no-daemon :can-cache-integration-tests:dependencies \
    --configuration testRuntimeClasspath

COPY can-cache-integration-tests/src can-cache-integration-tests/src

CMD ["./gradlew", "--no-daemon", ":can-cache-integration-tests:test"]
