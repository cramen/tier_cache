plugins {
    `java-library`
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
    // Adds nativeCompile/nativeRun only; `build`/`check` are unaffected.
    // The main class is inherited from the Spring Boot plugin.
    alias(libs.plugins.graalvm.native)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    implementation(project(":tiercache-spring-boot-starter"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Demo is not part of `check`; it has its own verification task.
tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}
