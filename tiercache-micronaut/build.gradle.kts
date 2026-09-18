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
    // The Micronaut 4.x platform aligns micronaut-core/cache/test versions
    // (design D6 pins the platform line).
    implementation(platform(libs.micronaut.platform))
    annotationProcessor(platform(libs.micronaut.platform))
    api(libs.micronaut.inject)
    api(libs.micronaut.cache.core)
    annotationProcessor(libs.micronaut.inject.java)

    testImplementation(platform(libs.micronaut.platform))
    testAnnotationProcessor(platform(libs.micronaut.platform))
    testImplementation(libs.micronaut.test.junit5)
    testAnnotationProcessor(libs.micronaut.inject.java)
    testImplementation(testFixtures(project(":tiercache-core")))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.junit.jupiter)
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.yaml:snakeyaml:2.3")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.18")
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Publishing (release automation): shared Central Portal target, license,
// and scm metadata come from the root build script.
mavenPublishing {
    pom {
        name.set("tiercache-micronaut")
        description.set(
            "Micronaut integration for the Tiercache two-level cache:" +
                " CacheManager, SyncCache/AsyncCache adapters, configuration properties"
        )
    }
}
