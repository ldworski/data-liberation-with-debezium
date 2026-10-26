plugins {
    java
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

group = "pl.dworski"
version = "0.0.1-SNAPSHOT"
description = "order-assembler"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-jackson")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.apache.kafka:kafka-streams")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("started", "passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
