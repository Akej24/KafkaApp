FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY gradlew .
COPY gradle.properties .
COPY settings.gradle.kts .
COPY gradle gradle

COPY core/src ./core/src
COPY core/build.gradle.kts core/build.gradle.kts

RUN chmod +x gradlew
RUN ./gradlew dependencies --no-daemon

ENTRYPOINT ["./gradlew", "run", "--no-daemon"]
