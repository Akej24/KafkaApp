FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY gradlew .
COPY gradle.properties .
COPY gradle gradle

COPY consumer/src ./src
COPY consumer/build.gradle.kts ./build.gradle.kts
COPY consumer/settings.gradle.kts ./settings.gradle.kts

RUN chmod +x gradlew
RUN ./gradlew dependencies --no-daemon

ENTRYPOINT ["./gradlew", "run", "--no-daemon"]
