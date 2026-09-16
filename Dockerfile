FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY pom.xml .
COPY proto ./proto
COPY src ./src

RUN mvn -B package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY  --from=build /app/target/canais-java-1.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-cp", "app.jar"]
CMD ["sd.projeto.server.Server"]