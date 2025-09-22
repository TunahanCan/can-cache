FROM maven:3.9.9-eclipse-temurin-21

WORKDIR /workspace/tests

COPY pom.xml ./
RUN mvn -B dependency:go-offline

COPY src ./src

CMD ["mvn", "-B", "test"]
