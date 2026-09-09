FROM maven:3.9.6-eclipse-temurin-21-jammy AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-jammy AS production
WORKDIR /app
RUN groupadd -r appgroup && useradd -r -g appgroup -s /bin/false appuser
COPY --from=build --chown=appuser:appgroup /app/target/*.jar app.jar
USER appuser
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError", "-jar", "IMADDS-1.0.0.jar"]