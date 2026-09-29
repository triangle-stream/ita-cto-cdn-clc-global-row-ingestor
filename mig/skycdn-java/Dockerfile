FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -DskipTests clean package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/skycdn-mig-worker-1.0.0.jar /app/worker.jar
ENTRYPOINT ["java","-XX:+UseG1GC","-jar","/app/worker.jar"]
