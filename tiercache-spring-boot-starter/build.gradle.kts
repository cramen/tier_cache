plugins {
    `java-library`
    alias(libs.plugins.vanniktech.publish)
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

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-spring-boot-starter")
        description.set(
            "Spring Boot starter for the Tiercache two-level cache:" +
                " auto-configuration and Spring Cache SPI adapters"
        )
    }
}

// A JVM inside the fixture network reaches the exact addresses advertised by Sentinel.
tasks.register<Sync>("sentinelRuntime") {
    dependsOn(tasks.testClasses)
    into(layout.buildDirectory.dir("sentinel-runtime"))
    from(sourceSets.test.get().output) { into("classes") }
    from(sourceSets.main.get().output) { into("classes") }
    from(configurations.testRuntimeClasspath) { into("lib") }
}
