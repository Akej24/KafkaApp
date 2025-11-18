FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY gradlew .
COPY gradle.properties .
COPY gradle gradle

COPY kafka/src ./src
COPY kafka/build.gradle.kts ./build.gradle.kts
COPY kafka/settings.gradle.kts ./settings.gradle.kts

RUN chmod +x gradlew
RUN ./gradlew dependencies --no-daemon

EXPOSE 34045
ENTRYPOINT ["./gradlew", "run", "--no-daemon"]
