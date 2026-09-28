FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -DskipTests clean package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/target/skycdn-cloudrun-ingestor-1.0.0.jar /app/app.jar
ENV PORT=8080
CMD ["java", "-XX:+UseG1GC", "-jar", "/app/app.jar"]
