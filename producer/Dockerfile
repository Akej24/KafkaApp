FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY gradlew .
COPY gradle.properties .
COPY gradle gradle

COPY producer/src ./src
COPY producer/build.gradle.kts ./build.gradle.kts
COPY producer/settings.gradle.kts ./settings.gradle.kts

RUN chmod +x gradlew
RUN ./gradlew dependencies --no-daemon

ENTRYPOINT ["./gradlew", "run", "--no-daemon"]
