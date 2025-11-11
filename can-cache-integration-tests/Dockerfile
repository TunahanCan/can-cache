FROM maven:3.9.11-eclipse-temurin-25

WORKDIR /workspace/tests

COPY pom.xml ./
RUN mvn -B dependency:go-offline

COPY src ./src

CMD ["mvn", "-B", "test"]
