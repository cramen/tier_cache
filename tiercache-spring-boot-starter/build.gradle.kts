plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api(project(":tiercache-core"))
    api(project(":tiercache-transport-redis"))
    api(project(":tiercache-invalidation"))
    compileOnly(project(":tiercache-micrometer"))
    compileOnly(libs.micrometer.core)
    testImplementation(project(":tiercache-micrometer"))
    testImplementation(libs.micrometer.core)
    api(libs.spring.boot.starter)
    api(libs.spring.boot.starter.cache)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Spring SpEL (`#id`) in annotations needs parameter names at runtime.
tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}
