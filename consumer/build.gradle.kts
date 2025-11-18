// https://docs.gradle.org/9.1.0/userguide/building_java_projects.html

plugins {
    java
    application
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("org.example.App")
}

dependencies {
    testImplementation(libs.junit)
    implementation(libs.logbackClassic)
    implementation(libs.springKafka)
    implementation(libs.springKafkaTest)
    implementation(libs.testcontainers)
    implementation(libs.testcontainersPostgresql)
    implementation(libs.postgresql)
}

repositories {
    mavenCentral()
}

tasks.withType<JavaExec> {
    standardInput = System.`in`
}
