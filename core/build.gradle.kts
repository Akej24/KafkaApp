// https://docs.gradle.org/9.1.0/userguide/building_java_projects.html

plugins {
    java
    application
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

application {
    mainClass.set("App")
}

dependencies {
    implementation(libs.kafkaClients)
    implementation(libs.springKafka)
    implementation(libs.springKafkaTest)
    implementation(libs.logbackClassic)
    testImplementation(libs.junit)
}

repositories {
    mavenCentral()
}
