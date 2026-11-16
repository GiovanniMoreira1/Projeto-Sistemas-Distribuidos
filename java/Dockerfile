FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app/java

COPY proto/client.proto /app/proto/client.proto
COPY java/pom.xml .
COPY java/src ./src

RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/java/target/canais-java-1.0-SNAPSHOT.jar app.jar
COPY java/usuarios.json ./

ENTRYPOINT ["java", "-cp", "/app/app.jar"]
CMD ["sd.projeto.server.Server"]
