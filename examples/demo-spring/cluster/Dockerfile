FROM eclipse-temurin:17-jre
WORKDIR /app
COPY build/libs/demo-spring-*-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
