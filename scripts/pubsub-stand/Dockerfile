FROM eclipse-temurin:17-jre
WORKDIR /app
COPY classes classes
COPY lib lib
ENTRYPOINT ["java", "--add-modules", "jdk.httpserver", "-Xms128m", "-Xmx384m", "-XX:ActiveProcessorCount=2", "-cp", "classes:lib/*", "io.tiercache.redis.PubSubStandNode"]
